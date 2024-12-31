# nsq-rs
A realtime distributed messaging platform inspired by NSQ writen in Rust


# 协议(V2)

- 投递消息
    - 请求体：`PUB <TOPIC-NAME>\n<BODY-SIZE><MSG-BODY>`
        - BODY-SIZE：消息体长度（字节），4Bytes，大端序
    - 响应体：OK
- 消息ACK
    - 请求体：`FIN <MSG-ID>\n`
    - 响应体：无
- 消息requeue
    - 请求体： `REQ <MSG-ID> <MS>\n`
        - MS：Base10编码（即数字字符串）的毫秒数
    - 响应体：无
- 批量投递消息
    - 请求体：`MPUB <TOPIC-NAME>\n<MSG-COUNT><BODY-SIZE-1><MSG-BODY-1><BODY-SIZE-2><MSG-BODY-2>...`
    - 响应体：OK
- 投递延迟消息
    - 请求体：`DPUB <TOPIC-NAME> <MS>\n<BODY-SIZE><MSG-BODY>`
        - MS：Base10编码（即数字字符串）的毫秒数
    - 响应体：OK
- 空操作
    - 请求体：`NOP\n`
    - 响应体：无
- 重置超时时间：针对已发送未确认的消息
    - 请求体：`TOUCH <MSG-ID>\n`
        - 对于一条已发送未确认的消息，如果其原本超时时间为10s，则在touch操作10s重新发送给客户端
    - 响应体：无
- 订阅
    - 请求体：`SUB <TOPIC-NAME> <CHANNEL-NAME>\n`
    - 响应体：OK
- client ready（只允许用在订阅后的操作）
    - 请求体：`RDY <MSG-ID> <MSG-COUNT>\n`
        - MSG-COUNT：Base10编码（即数字字符串）的消息数量
            当客户端准备好接收消息时上报一个数值，当客户端未确认的消息数量超过此数值，将会暂停发送
    - 响应体：无
- 取消订阅
    - 请求体：`CLS\n`
    - 响应体：`CLOSE_WAIT`