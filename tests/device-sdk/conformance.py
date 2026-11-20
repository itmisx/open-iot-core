#!/usr/bin/env python3
"""用真实设备 SDK 对跑 openiotd。由 run.sh 起节点并调用，不单独运行。

分工是刻意的：
  · 接入、策略、影子、吊销、ALPN、MQTT 5 用 AWS IoT Device SDK ——
    这些都是"真实设备怎么连"的问题，换个客户端验证就失去了意义。
  · QoS 2、遗嘱、保留消息、持久会话用 paho —— AWS IoT Core 不支持
    QoS 2，它的 SDK 因此不开放；这些是 open-iot-core 的超集行为，
    用通用客户端验证更直接。

断言的写法有一条纪律：**"被拒绝"只认 SDK 抛出的协议层错误**。
只要抛异常就算拒绝是不行的——脚本自己写错也会抛，那会把一条没跑成的
用例记成通过，而"拒绝"恰恰是安全结论，假通过比假失败危险得多。
"""
import json
import os
import ssl
import subprocess
import sys
import threading
import time

import paho.mqtt.client as paho
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from awscrt import io, mqtt, mqtt5
from awscrt.exceptions import AwsCrtError
from awscrt.mqtt import SubscribeError
from awsiot import iotshadow, mqtt5_client_builder, mqtt_connection_builder

WORK = os.environ['OPENIOT_WORK']
MQTT_PORT = int(os.environ['OPENIOT_MQTT_PORT'])
ALPN_PORT = int(os.environ['OPENIOT_ALPN_PORT'])
ADMIN = f"http://127.0.0.1:{os.environ['OPENIOT_ADMIN_PORT']}/api/admin/v1"
TOKEN = os.environ['OPENIOT_TOKEN']
WITH_KEEPALIVE = os.environ.get('WITH_KEEPALIVE') == '1'

# 服务端证书的 SAN 是 localhost 与 127.0.0.1，用 localhost 让 SDK 做主机名校验
HOST = 'localhost'

results = []


def check(name, fn):
    t0 = time.time()
    try:
        results.append(('PASS', name, fn() or '', time.time() - t0))
    except Exception as e:  # noqa: BLE001 — 对跑脚本，任何异常都是一条结果
        results.append(('FAIL', name, f'{type(e).__name__}: {e}', time.time() - t0))


def denied(fn, what):
    """断言 broker 拒绝了某个操作，且是协议层面的拒绝。"""
    try:
        out = fn()
    except (AwsCrtError, SubscribeError) as e:
        return f"{what}被拒：{getattr(e, 'name', type(e).__name__)}"
    raise AssertionError(f'{what}竟被放行：{out}')


def paths(thing):
    d = os.path.join(WORK, thing)
    return (os.path.join(d, f'{thing}.pem'),
            os.path.join(d, f'{thing}.key'),
            os.path.join(d, 'ca-chain.pem'))


def admin(method, path, body=None):
    cmd = ['curl', '-sf', '-X', method, '-H', f'Authorization: Bearer {TOKEN}']
    if body is not None:
        cmd += ['-H', 'Content-Type: application/json', '-d', json.dumps(body)]
    cmd.append(ADMIN + path)
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f'admin {method} {path} 失败：rc={r.returncode} {r.stderr.strip()}')
    return json.loads(r.stdout) if r.stdout.strip() else None


def connect(thing, client_id=None, on_down=None, port=MQTT_PORT):
    """用签发时原样的证书与私钥建立连接。

    不做任何格式转换是这里的要点之一：私钥的 PEM 外层编码曾经让
    AWS SDK 根本加载不了（PKCS#8 vs SEC1），而那条错误完全指不到原因。
    任何"先用 openssl 转一下"的便利写法都会把这个回归藏起来。
    """
    cert, key, ca = paths(thing)
    conn = mqtt_connection_builder.mtls_from_path(
        endpoint=HOST, port=port,
        cert_filepath=cert, pri_key_filepath=key, ca_filepath=ca,
        client_id=client_id or thing, clean_session=True, keep_alive_secs=30,
        reconnect_min_timeout_secs=60, reconnect_max_timeout_secs=60,
        # 服务端单方面断开走 interrupted；closed 只在彻底关闭后触发，
        # 用后者等"被踢"会一直等不到
        on_connection_interrupted=(lambda connection, error, **kw: on_down())
        if on_down else None,
    )
    conn.connect().result(10)
    return conn


# --------------------------------------------------------------- 1. 接入

state = {}


def t_connect():
    state['open'] = connect('dev-open')
    state['scoped'] = connect('dev-scoped')
    key_head = open(paths('dev-open')[1]).readline().strip()
    return f'mTLS {MQTT_PORT}，私钥按签发原样加载（{key_head}）'


check('CONNECT：mTLS 握手，私钥不经转换', t_connect)


def roundtrip(qos, label):
    def run():
        got = threading.Event()
        box = {}
        topic = f'conformance/{label}'
        sub, _ = state['open'].subscribe(
            topic=topic, qos=qos,
            callback=lambda topic, payload, **kw: (box.update({'p': payload}), got.set()))
        suback = sub.result(10)
        state['open'].publish(topic=topic, payload=b'{"v":1}', qos=qos)[0].result(10)
        if not got.wait(10):
            raise AssertionError('没有收到自己发布的消息')
        return f"SUBACK qos={int(suback['qos'])}，往返 {box['p']!r}"
    return run


check('QoS 0 发布订阅往返', roundtrip(mqtt.QoS.AT_MOST_ONCE, 'q0'))
check('QoS 1 发布订阅往返（PUBACK）', roundtrip(mqtt.QoS.AT_LEAST_ONCE, 'q1'))


# ----------------------------------------------------- 2. 策略（IoT Policy）

def t_denied_subscribe():
    def go():
        sub, _ = state['scoped'].subscribe(topic='devices/dev-open/#',
                                           qos=mqtt.QoS.AT_LEAST_ONCE,
                                           callback=lambda **kw: None)
        return sub.result(10)
    return denied(go, '越权订阅其他设备的主题')


check('策略：越权订阅被 SUBACK 拒', t_denied_subscribe)


def t_clientid_mismatch():
    def go():
        bad = connect('dev-scoped', client_id='rogue-client')
        bad.disconnect().result(5)
        return 'connected'
    return denied(go, 'clientId 与证书主体不符的连接')


check('策略：clientId 与证书主体不符被拒连', t_clientid_mismatch)


# -------------------------------------------------------------- 3. 影子

def t_shadow():
    """走 SDK 的 iotshadow 客户端。

    它的主题布局与 JSON 契约是按 AWS 硬编码的，跑通意味着契约真的对上了，
    而不只是"我们自己的实现自洽"。
    """
    name = 'dev-scoped'
    sh = iotshadow.IotShadowClient(state['scoped'])
    accepted, rejected, delta = {}, {}, {}
    got_get, got_delta = threading.Event(), threading.Event()

    sh.subscribe_to_get_shadow_accepted(
        request=iotshadow.GetShadowSubscriptionRequest(thing_name=name),
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=lambda r: (accepted.update({'r': r}), got_get.set()))[0].result(10)
    sh.subscribe_to_get_shadow_rejected(
        request=iotshadow.GetShadowSubscriptionRequest(thing_name=name),
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=lambda e: (rejected.update({'e': e}), got_get.set()))[0].result(10)
    sh.subscribe_to_shadow_delta_updated_events(
        request=iotshadow.ShadowDeltaUpdatedSubscriptionRequest(thing_name=name),
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=lambda ev: (delta.update({'ev': ev}), got_delta.set()))[0].result(10)

    # 设备先上报 reported，再由管理面改 desired，设备应当收到 delta
    sh.publish_update_shadow(iotshadow.UpdateShadowRequest(
        thing_name=name,
        state=iotshadow.ShadowState(reported={'interval': 30, 'mode': 'run'})),
        qos=mqtt.QoS.AT_LEAST_ONCE).result(10)
    time.sleep(0.5)

    sh.publish_get_shadow(iotshadow.GetShadowRequest(thing_name=name),
                          qos=mqtt.QoS.AT_LEAST_ONCE).result(10)
    if not got_get.wait(10):
        raise AssertionError(f'$aws/things/{name}/shadow/get 没有任何响应')
    if 'e' in rejected:
        raise AssertionError(f"get 被拒：{rejected['e'].code} {rejected['e'].message}")
    reported = accepted['r'].state.reported
    if not reported or reported.get('mode') != 'run':
        raise AssertionError(f'get/accepted 的 reported 不对：{reported}')

    admin('PUT', f'/things/{name}/shadow',
          {'state': {'desired': {'interval': 5, 'mode': 'burst'}}})
    if not got_delta.wait(10):
        raise AssertionError('改了 desired 却没有收到 delta')
    d = delta['ev'].state
    if d.get('interval') != 5:
        raise AssertionError(f'delta 内容不对：{d}')
    return f"reported={reported}，delta={d}，version={delta['ev'].version}"


check('影子：update / get / delta（SDK 的 iotshadow）', t_shadow)


# ------------------------------------------------- 4. 吊销即时断连（PKI-030）

def t_revoke():
    """用一次性设备验证，不动前面几台。

    吊销是终态，拿正在用的设备来试会让后面的用例从一个被吊销的状态开始。
    """
    name = 'dev-doomed'
    admin('PUT', f'/things/{name}', {'policyName': 'conformance-open'})
    issued = admin('POST', '/certificates/issue',
                   {'thing': name, 'policyName': 'conformance-open'})
    fp = issued['fingerprint']
    d = os.path.join(WORK, name)
    os.makedirs(d, exist_ok=True)
    for fname, content in ((f'{name}.pem', issued['certificatePem']),
                           (f'{name}.key', issued['privateKeyPem']),
                           ('ca-chain.pem', issued['chainPem'])):
        with open(os.path.join(d, fname), 'w') as f:
            f.write(content)
    os.chmod(os.path.join(d, f'{name}.key'), 0o600)

    try:
        down = threading.Event()
        conn = connect(name, on_down=down.set)
        t0 = time.time()
        admin('PUT', f'/certificates/{fp}/status',
              {'status': 'REVOKED', 'reason': '设备 SDK 对照测试'})
        hit = down.wait(10)
        dt = time.time() - t0
        try:
            conn.disconnect().result(3)
        except Exception:
            pass
        if not hit:
            raise AssertionError('吊销后 10 秒内连接仍在，PKI-030 要求即时断连')
        return f'吊销后 {dt:.2f}s 内被服务端断开'
    finally:
        for method, path in (('DELETE', f'/certificates/{fp}'),
                             ('DELETE', f'/things/{name}')):
            try:
                admin(method, path)
            except Exception as e:
                print(f'清理 {path} 失败：{e}', file=sys.stderr)


check('吊销即时断连（PKI-030）', t_revoke)


# ----------------------------------------------- 5. ALPN 端点（NET-002）

def t_alpn():
    """443 上以 ALPN x-amzn-mqtt-ca 协商 MQTT。

    受限网络里 443 常是唯一出口，这是 AWS 设备的常规走法，不是边角功能。
    这里用高位端口代 443（非 root 绑不了 443），协商过程完全一致。
    """
    if not io.is_alpn_available():
        raise AssertionError('本机 awscrt 不支持 ALPN，无法验证')
    cert, key, ca = paths('dev-open')
    tls = io.TlsContextOptions.create_client_with_mtls_from_path(cert, key)
    tls.override_default_trust_store_from_path(None, ca)
    tls.alpn_list = ['x-amzn-mqtt-ca']
    conn = mqtt.Connection(
        client=mqtt.Client(io.ClientBootstrap.get_or_create_static_default(),
                           io.ClientTlsContext(tls)),
        client_id='dev-open-alpn', host_name=HOST, port=ALPN_PORT,
        clean_session=True, keep_alive_secs=30)
    conn.connect().result(10)
    got = threading.Event()
    conn.subscribe(topic='conformance/alpn', qos=mqtt.QoS.AT_LEAST_ONCE,
                   callback=lambda **kw: got.set())[0].result(10)
    conn.publish(topic='conformance/alpn', payload=b'hi',
                 qos=mqtt.QoS.AT_LEAST_ONCE)[0].result(10)
    ok = got.wait(10)
    conn.disconnect().result(5)
    if not ok:
        raise AssertionError('ALPN 连上了但收发不通')
    return f'{ALPN_PORT} 上以 x-amzn-mqtt-ca 协商成功并完成收发'


check('ALPN x-amzn-mqtt-ca 端点（NET-002）', t_alpn)


# ----------------------------------------------------- 6. MQTT 5 客户端

def t_mqtt5():
    """SDK v2 的新设备默认走 mqtt5，与 3.1.1 是两套实现，必须分别验证。"""
    cert, key, ca = paths('dev-open')
    connected, got = threading.Event(), threading.Event()
    box = {}
    client = mqtt5_client_builder.mtls_from_path(
        endpoint=HOST, port=MQTT_PORT,
        cert_filepath=cert, pri_key_filepath=key, ca_filepath=ca,
        client_id='dev-open-m5',
        on_lifecycle_connection_success=lambda d: (
            box.update({'connack': d.connack_packet}), connected.set()),
        on_publish_received=lambda d: (box.update({'msg': d.publish_packet}), got.set()))
    client.start()
    if not connected.wait(10):
        raise AssertionError('mqtt5 未能建立连接')
    suback = client.subscribe(mqtt5.SubscribePacket(subscriptions=[
        mqtt5.Subscription(topic_filter='conformance/m5',
                           qos=mqtt5.QoS.AT_LEAST_ONCE)])).result(10)
    client.publish(mqtt5.PublishPacket(topic='conformance/m5', payload=b'hello',
                                       qos=mqtt5.QoS.AT_LEAST_ONCE)).result(10)
    ok = got.wait(10)
    client.stop()
    if not ok:
        raise AssertionError('mqtt5 收发不通')
    return (f"CONNACK={box['connack'].reason_code.name}，"
            f'SUBACK={suback.reason_codes[0].name}，收到 {box["msg"].payload!r}')


check('MQTT 5 客户端（SDK v2 默认路径）', t_mqtt5)


# ------------------------------- 7. paho：QoS 2、保留消息、遗嘱、持久会话

def paho_client(cid, clean=True, will=None, keepalive=30, v5=False, on_message=None,
                thing='dev-open'):
    cert, key, ca = paths(thing)
    c = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=cid,
                    protocol=paho.MQTTv5 if v5 else paho.MQTTv311,
                    clean_session=None if v5 else clean)
    c.tls_set(ca_certs=ca, certfile=cert, keyfile=key,
              cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    if will:
        c.will_set(will[0], will[1], qos=1, retain=False)
    # on_message 必须在 connect 之前挂上。
    #
    # 离线队列的补投是紧跟 CONNACK 过来的：连上之后再挂回调会漏掉它，
    # 而那看起来和"服务端没补投"一模一样。这个竞态真的误报过一次，
    # 差点被当成丢消息的缺陷去查。
    if on_message:
        c.on_message = on_message
    ev, info = threading.Event(), {}
    c.on_connect = lambda cl, ud, flags, rc, props=None: (
        info.update({'flags': flags}), ev.set())
    c.connect(HOST, MQTT_PORT, keepalive=keepalive)
    c.loop_start()
    if not ev.wait(10):
        raise AssertionError(f'{cid} 连接超时')
    return c, info


def watcher(topic, qos=1):
    got, box = threading.Event(), {}
    w, _ = paho_client('watch-' + topic.replace('/', '-'),
                       on_message=lambda cl, ud, m: (box.update({'m': m}), got.set()))
    w.subscribe(topic, qos=qos)
    time.sleep(0.3)
    return w, got, box


def t_qos2():
    """QoS 2 是超集：AWS IoT Core 只到 QoS 1，它的 SDK 不开放 QoS 2。

    "AWS 设备用不到"不等于"可以不对"——它在 PRD 里是 v0.1 条目。
    """
    # 订阅端也必须是 QoS 2：订阅 QoS 1 时下发会被降级，那样只验到了
    # 入站的 PUBREC/PUBREL/PUBCOMP，出站那一半没覆盖
    w, got, box = watcher('conformance/q2', qos=2)
    pub, _ = paho_client('q2-pub')
    pub.publish('conformance/q2', b'exactly-once', qos=2).wait_for_publish(10)
    ok = got.wait(10)
    for c in (w, pub):
        c.loop_stop(), c.disconnect()
    if not ok:
        raise AssertionError('QoS 2 消息没有送达')
    if box['m'].qos != 2:
        raise AssertionError(f"下发 QoS = {box['m'].qos}，两端都要 QoS 2 才算走通")
    return f"PUBREC/PUBREL/PUBCOMP 两端走通，收到 {box['m'].payload!r} qos={box['m'].qos}"


check('QoS 2 端到端（超集）', t_qos2)


def t_retained():
    pub, _ = paho_client('ret-pub')
    pub.publish('conformance/ret', b'sticky', qos=1, retain=True).wait_for_publish(10)
    time.sleep(0.3)
    w, got, box = watcher('conformance/ret')
    ok = got.wait(10)
    pub.publish('conformance/ret', b'', qos=1, retain=True).wait_for_publish(5)
    for c in (w, pub):
        c.loop_stop(), c.disconnect()
    if not ok:
        raise AssertionError('新订阅者没有收到保留消息')
    return f"新订阅者立即收到 {box['m'].payload!r}，retain={box['m'].retain}"


check('保留消息', t_retained)


def t_will_abrupt():
    """连接被掐断时必须发遗嘱（MQTT-060、MQTT-3.1.2.5）。

    这是遗嘱存在的全部理由。曾经只有 MQTT 5 的 DISCONNECT reason 0x04
    会触发，也就是只在客户端**主动请求**时才发，恰好把用途反过来了。
    """
    w, got, box = watcher('conformance/will')
    d, _ = paho_client('die-abrupt', will=('conformance/will', b'abrupt'))
    d.loop_stop()
    d._sock.close()  # 不发 DISCONNECT，直接掐掉——设备掉电的真实情形
    ok = got.wait(10)
    w.loop_stop(), w.disconnect()
    if not ok:
        raise AssertionError('连接被掐断后没有收到遗嘱')
    return f"收到遗嘱 {box['m'].payload!r}"


check('遗嘱：连接被掐断（MQTT-060）', t_will_abrupt)


def t_will_clean_disconnect():
    w, got, box = watcher('conformance/will-clean')
    d, _ = paho_client('die-polite', will=('conformance/will-clean', b'polite'))
    d.loop_stop(), d.disconnect()
    fired = got.wait(5)
    w.loop_stop(), w.disconnect()
    if fired:
        raise AssertionError('正常 DISCONNECT 不该触发遗嘱（MQTT-063）')
    return '正常 DISCONNECT 不发遗嘱'


check('遗嘱：正常 DISCONNECT 不发（MQTT-063）', t_will_clean_disconnect)


def t_will_keepalive():
    """保活超时发遗嘱。

    读超时是 max(客户端请求, 30s) × 1.5 = 45 秒，所以这条必须等够——
    等不够会得到一个假的"没有遗嘱"。默认跳过，WITH_KEEPALIVE=1 开启。
    """
    if not WITH_KEEPALIVE:
        return '跳过（WITH_KEEPALIVE=1 开启，约需 45 秒）'
    w, got, box = watcher('conformance/will-ka')
    d, _ = paho_client('die-ka', will=('conformance/will-ka', b'keepalive'))
    d.loop_stop()  # 不再发 PINGREQ，socket 仍开着
    t0 = time.time()
    ok = got.wait(70)
    try:
        d._sock.close()
    except Exception:
        pass
    w.loop_stop(), w.disconnect()
    if not ok:
        raise AssertionError('保活超时后没有收到遗嘱')
    return f'保活超时 {time.time() - t0:.0f}s 后收到遗嘱'


check('遗嘱：保活超时（MQTT-060）', t_will_keepalive)


def will_delay_client(cid, topic, payload, delay_secs, expiry_secs=3600):
    """MQTT 5 带 Will Delay Interval 的连接。

    延迟是 MQTT 5 的遗嘱属性，3.1.1 没有，所以这条只能走 v5。
    """
    cert, key, ca = paths('dev-open')
    c = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=cid,
                    protocol=paho.MQTTv5)
    c.tls_set(ca_certs=ca, certfile=cert, keyfile=key,
              cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    wp = Properties(PacketTypes.WILLMESSAGE)
    wp.WillDelayInterval = delay_secs
    c.will_set(topic, payload, qos=1, retain=False, properties=wp)
    cp = Properties(PacketTypes.CONNECT)
    cp.SessionExpiryInterval = expiry_secs
    ev = threading.Event()
    c.on_connect = lambda cl, ud, flags, rc, props=None: ev.set()
    c.connect(HOST, MQTT_PORT, keepalive=30, clean_start=False, properties=cp)
    c.loop_start()
    if not ev.wait(10):
        raise AssertionError(f'{cid} 连接超时')
    return c


def t_will_delay():
    """Will Delay Interval（MQTT-061）：延迟窗口内不发，走完才发。

    延迟的用意是"短暂掉线不要惊动别人"：设备网络抖一下就广播一条下线，
    比不发还糟——告警会被噪声淹没。
    """
    w, got, box = watcher('conformance/will-delay')
    d = will_delay_client('die-delayed', 'conformance/will-delay', b'delayed', 4)
    d.loop_stop()
    d._sock.close()
    early = got.wait(2)
    if early:
        w.loop_stop(), w.disconnect()
        raise AssertionError('延迟还没走完就发了遗嘱')
    ok = got.wait(10)
    w.loop_stop(), w.disconnect()
    if not ok:
        raise AssertionError('延迟走完后仍未收到遗嘱')
    return f"2s 时未发、延迟走完后收到 {box['m'].payload!r}"


check('遗嘱：Will Delay 推迟发布（MQTT-061）', t_will_delay)


def t_will_delay_cancelled():
    """窗口内重连必须取消遗嘱（MQTT-3.1.2.5）。

    不取消的话，延迟只是把那条噪声推迟了几秒，一点用都没有。
    """
    w, got, box = watcher('conformance/will-cancel')
    d = will_delay_client('die-returns', 'conformance/will-cancel', b'should-not-fire', 6)
    d.loop_stop()
    d._sock.close()
    time.sleep(1)
    back = will_delay_client('die-returns', 'conformance/will-cancel', b'should-not-fire', 6)
    fired = got.wait(10)  # 推过原定触发点
    back.loop_stop(), back.disconnect()
    w.loop_stop(), w.disconnect()
    if fired:
        raise AssertionError(f"重连之后仍发出了遗嘱：{box['m'].payload!r}")
    return '窗口内重连，遗嘱被取消'


check('遗嘱：窗口内重连取消（MQTT-3.1.2.5）', t_will_delay_cancelled)


def t_will_topic_authz():
    """遗嘱主题要过发布授权（MQTT-068）。

    遗嘱是客户端在 CONNECT 里指定的任意主题，服务端替它发。不判的话，
    一台被收敛在自己名下的设备，可以把遗嘱设到别的设备的主题上，
    断一次线就让服务端代发——凭据是自己的，权限却是别人的。

    这个缺口原先够不着（遗嘱只在客户端主动要求时才发），补上"掉线也发"
    之后才真正打开。
    """
    w, got, box = watcher('devices/dev-open/forged')
    # dev-scoped 的策略只覆盖 devices/dev-scoped/*
    bad, _ = paho_client('dev-scoped', thing='dev-scoped',
                         will=('devices/dev-open/forged', b'forged'))
    bad.loop_stop()
    bad._sock.close()
    fired = got.wait(6)
    w.loop_stop(), w.disconnect()
    if fired:
        raise AssertionError(f"越权的遗嘱竟被发出：{box['m'].payload!r}")

    # 反向确认：自己名下的遗嘱仍然正常发出，不能因为加了判定把合法的也挡了。
    w2, got2, box2 = watcher('devices/dev-scoped/offline')
    good, _ = paho_client('dev-scoped', thing='dev-scoped',
                          will=('devices/dev-scoped/offline', b'legit'))
    good.loop_stop()
    good._sock.close()
    ok = got2.wait(10)
    w2.loop_stop(), w2.disconnect()
    if not ok:
        raise AssertionError('策略内的遗嘱也被挡住了')
    return f"越权遗嘱被丢弃，自己名下的 {box2['m'].payload!r} 正常发出"


check('遗嘱：越权主题被丢弃（MQTT-068）', t_will_topic_authz)


def t_persistent_session():
    cid = 'persist-01'
    c, _ = paho_client(cid, clean=False)
    c.subscribe('conformance/persist', qos=1)
    time.sleep(0.3)
    c.loop_stop(), c.disconnect()
    time.sleep(0.3)

    pub, _ = paho_client('persist-pub')
    pub.publish('conformance/persist', b'while-offline', qos=1).wait_for_publish(10)
    pub.loop_stop(), pub.disconnect()
    time.sleep(0.3)

    got, box = threading.Event(), {}
    again, info = paho_client(cid, clean=False,
                              on_message=lambda cl, ud, m: (box.update({'m': m}), got.set()))
    ok = got.wait(10)
    present = info['flags'].session_present
    again.loop_stop(), again.disconnect()
    if not present:
        raise AssertionError('重连时 session_present=0，会话没有被保留')
    if not ok:
        raise AssertionError('会话保留了，但离线期间的 QoS 1 消息没有补投')
    return f"session_present=1，补投 {box['m'].payload!r}"


check('持久会话与离线消息补投', t_persistent_session)


# --------------------------------------------------------------- 收尾与报告

for conn in state.values():
    try:
        conn.disconnect().result(5)
    except Exception:
        pass

width = max(len(r[1]) for r in results)
print()
for status, name, detail, dt in results:
    mark = '\033[1;32mPASS\033[0m' if status == 'PASS' else '\033[1;31mFAIL\033[0m'
    print(f'{mark}  {name.ljust(width)}  {dt:5.2f}s  {detail}')
print()
bad = sum(1 for r in results if r[0] == 'FAIL')
print(f'{len(results) - bad}/{len(results)} 通过')
sys.exit(1 if bad else 0)
