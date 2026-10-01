# -*- coding: utf-8 -*-
"""本地抓包 gRPC 消息解析测试的资源目录。

- ``_proto_index.py``：GrpcProto 下全部 ``*_pb2.py`` 的静态索引
  （方法 -> 请求/响应消息类，protobuf 全限定名 -> 消息类），自动生成；
- ``_resolver.py``：按「方法名」解析请求/响应消息类；
- ``_loader.py``：抓包原始报文 -> protobuf 消息的加载/去帧/解压/解析工具；
- ``<Method>/``：以 gRPC 方法名命名的抓包目录，内含 ``request/`` 与 ``response/``。
"""
