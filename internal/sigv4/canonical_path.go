package sigv4

import "strings"

/*
规范路径的编码规则。

AWS 的 SigV4（除 S3 之外）对路径做**两次** URI 编码：客户端拿自己已经编码
过的路径，再整体编一遍。两个官方 SDK 都是这么做的，而且是在各自的代码里
各自做的：

  - botocore：quote(normalize_url_path(path), safe='/~')，输入是已编码的路径，
    于是 % 变成 %25。
  - aws-sdk-go-v2：rest.EscapePath(GetURIPath(url), false)，输入是 URL 的
    RawPath，同样再编一遍。

由此，线上是 `cert%2Fab` 的那一段，签名里是 `cert%252Fab`。

**修订说明**：这里原先的实现是「把每一段先解码再编回去」，结果仍是 %2F
（只编一次），而 AUTH-077 的原文也是这么写的。那条规则是推断出来的，不是
对着真 SDK 量出来的：当时的用例自己构造规范请求再自己验，两边用同一套错
规则，所以永远是绿的。真跑起来的后果是**两类请求对任何 SDK 都 401**：

  - 数据面的 Publish：主题整段进路径，`demo/topic` 在线上是 `demo%2Ftopic`。
  - 控制面凡是把 ARN 放在路径里的（ListAttachedPolicies、ListTargetsForPolicy），
    ARN 里的斜杠在线上是 %2F。

而 401 的字面意思是「密钥不对或时钟偏了」，没有一个字指向编码——这正是
AUTH-075 刻意把「密钥不存在」和「签名不对」合成同一个拒绝的代价：它让这类
缺陷的现象变得与真因无关。所以这条规则现在由 tests/aws-sdk 用真 boto3
与真 aws CLI 覆盖（TST-129 扩项），不再只有自对自的单元测试。

分段边界仍按**原始**路径切（AUTH-077 的另一半依然成立）：段内的 %2F 在原始
串里不是斜杠，不会被当成新的一段。
*/

// canonicalPath 把路径编成规范 URI。空路径是 /。
func canonicalPath(path string) string {
	return canonicalPathRaw(path, "")
}

// canonicalPathRaw 按原始请求的分段边界编码，并且不解码段内已有的百分号
// 转义——那一层正是 SDK 自己又编了一遍的那一层。
func canonicalPathRaw(path, rawPath string) string {
	src := path
	if rawPath != "" {
		src = rawPath
	}
	if src == "" {
		return "/"
	}
	if !strings.HasPrefix(src, "/") {
		src = "/" + src
	}
	parts := strings.Split(src, "/")
	for i, p := range parts {
		parts[i] = awsEncode(p)
	}
	return strings.Join(parts, "/")
}
