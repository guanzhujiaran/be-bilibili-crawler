# -*- coding: utf-8 -*-
"""GrpcProto 下全部 protobuf 模块的静态索引（AUTO-GENERATED，请勿手改）。

由 ``test/grpc_capture/README.md`` 中的「重新生成索引」命令生成。作用：

- ``METHOD_INDEX``：gRPC 方法名 -> :class:`ProtoMethod`（含请求/响应消息类），
  用于按「方法名文件夹」自动解析抓包报文的 proto 类型；
- ``MESSAGE_INDEX``：protobuf 全限定名 -> 消息类，用于按名解析单个消息
  （例如 gRPC header 里 ``-bin`` 后缀的 base64 消息）。

注意：``*_pb2.py`` 之间以顶层包名（``bilibili.*`` / ``pgc.*`` / ``datacenter.*``）
互相引用，因此这里也用顶层包名导入（依赖 ``test/conftest.py`` 把 GrpcProto
目录加入 ``sys.path``），避免同一文件被重复加载进 descriptor pool。

已被排除的模块：``bilibili/app/playerunite/pugvanymodel/proto/pugvanymodel_pb2.py``
（与 ``bilibili/app/playerunite/pugvanymodel/pugvanymodel_pb2.py`` 符号重复，
同时导入会触发 descriptor pool 冲突）。
"""

from collections.abc import Iterator
from dataclasses import dataclass
from types import ModuleType

from google.protobuf.descriptor import Descriptor
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClass

import bilibili.account.fission.v1.fission_pb2 as _pb2_0
import bilibili.account.service.v1.service_pb2 as _pb2_1
import bilibili.ad.v1.ad_pb2 as _pb2_2
import bilibili.api.player.v1.player_pb2 as _pb2_3
import bilibili.api.probe.v1.probe_pb2 as _pb2_4
import bilibili.api.ticket.v1.ticket_pb2 as _pb2_5
import bilibili.app.archive.middleware.v1.preload_pb2 as _pb2_6
import bilibili.app.archive.v1.archive_pb2 as _pb2_7
import bilibili.app.card.v1.ad_pb2 as _pb2_8
import bilibili.app.card.v1.card_pb2 as _pb2_9
import bilibili.app.card.v1.common_pb2 as _pb2_10
import bilibili.app.card.v1.double_pb2 as _pb2_11
import bilibili.app.card.v1.single_pb2 as _pb2_12
import bilibili.app.click.v1.heartbeat_pb2 as _pb2_13
import bilibili.app.coldstart.v1.coldstart_pb2 as _pb2_14
import bilibili.app.distribution.setting.download_pb2 as _pb2_15
import bilibili.app.distribution.setting.dynamic_pb2 as _pb2_16
import bilibili.app.distribution.setting.experimental_pb2 as _pb2_17
import bilibili.app.distribution.setting.internaldevice_pb2 as _pb2_18
import bilibili.app.distribution.setting.night_pb2 as _pb2_19
import bilibili.app.distribution.setting.other_pb2 as _pb2_20
import bilibili.app.distribution.setting.pegasus_pb2 as _pb2_21
import bilibili.app.distribution.setting.play_pb2 as _pb2_22
import bilibili.app.distribution.setting.privacy_pb2 as _pb2_23
import bilibili.app.distribution.setting.search_pb2 as _pb2_24
import bilibili.app.distribution.v1.distribution_pb2 as _pb2_25
import bilibili.app.dynamic.common.dynamic_pb2 as _pb2_26
import bilibili.app.dynamic.v1.dynamic_pb2 as _pb2_27
import bilibili.app.dynamic.v2.campus_pb2 as _pb2_28
import bilibili.app.dynamic.v2.dynamic_pb2 as _pb2_29
import bilibili.app.dynamic.v2.opus_pb2 as _pb2_30
import bilibili.app.feed.v1.feed_pb2 as _pb2_31
import bilibili.app.growth.v1.growth_pb2 as _pb2_32
import bilibili.app.im.v1.im_pb2 as _pb2_33
import bilibili.app.interfaces.v1.history_pb2 as _pb2_34
import bilibili.app.interfaces.v1.media_pb2 as _pb2_35
import bilibili.app.interfaces.v1.search_pb2 as _pb2_36
import bilibili.app.interfaces.v1.space_pb2 as _pb2_37
import bilibili.app.listener.v1.listener_pb2 as _pb2_38
import bilibili.app.mine.v1.mine_pb2 as _pb2_39
import bilibili.app.nativeact.v1.nativeact_pb2 as _pb2_40
import bilibili.app.overseas.ad.v1.ad_pb2 as _pb2_41
import bilibili.app.playeronline.v1.playeronline_pb2 as _pb2_42
import bilibili.app.playerunite.pgcanymodel.pgcanymodel_pb2 as _pb2_43
import bilibili.app.playerunite.pugvanymodel.pugvanymodel_pb2 as _pb2_44
import bilibili.app.playerunite.ugcanymodel.ugcanymodel_pb2 as _pb2_45
import bilibili.app.playerunite.v1.playerunite_pb2 as _pb2_46
import bilibili.app.playurl.v1.playurl_pb2 as _pb2_47
import bilibili.app.resource.privacy.v1.api_pb2 as _pb2_48
import bilibili.app.resource.v1.module_pb2 as _pb2_49
import bilibili.app.search.v2.search_pb2 as _pb2_50
import bilibili.app.show.gateway.v1.service_pb2 as _pb2_51
import bilibili.app.show.mixture.v1.mixture_pb2 as _pb2_52
import bilibili.app.show.popular.v1.popular_pb2 as _pb2_53
import bilibili.app.show.rank.v1.rank_pb2 as _pb2_54
import bilibili.app.show.region.v1.region_pb2 as _pb2_55
import bilibili.app.space.v1.space_pb2 as _pb2_56
import bilibili.app.splash.v1.splash_pb2 as _pb2_57
import bilibili.app.topic.v1.topic_pb2 as _pb2_58
import bilibili.app.view.v1.view_pb2 as _pb2_59
import bilibili.app.viewunite.common_pb2 as _pb2_60
import bilibili.app.viewunite.pgcanymodel_pb2 as _pb2_61
import bilibili.app.viewunite.pugvanymodel_pb2 as _pb2_62
import bilibili.app.viewunite.ugcanymodel_pb2 as _pb2_63
import bilibili.app.viewunite.v1.viewunite_pb2 as _pb2_64
import bilibili.app.wall.v1.wall_pb2 as _pb2_65
import bilibili.broadcast.message.editor.notify_pb2 as _pb2_66
import bilibili.broadcast.message.esports.notify_pb2 as _pb2_67
import bilibili.broadcast.message.fission.notify_pb2 as _pb2_68
import bilibili.broadcast.message.im.notify_pb2 as _pb2_69
import bilibili.broadcast.message.main.dm_pb2 as _pb2_70
import bilibili.broadcast.message.main.native_pb2 as _pb2_71
import bilibili.broadcast.message.main.resource_pb2 as _pb2_72
import bilibili.broadcast.message.main.search_pb2 as _pb2_73
import bilibili.broadcast.message.note.sync_pb2 as _pb2_74
import bilibili.broadcast.message.ogv.freya_pb2 as _pb2_75
import bilibili.broadcast.message.ogv.live_pb2 as _pb2_76
import bilibili.broadcast.message.reply.reply_pb2 as _pb2_77
import bilibili.broadcast.message.ticket.activitygame_pb2 as _pb2_78
import bilibili.broadcast.message.tv.proj_pb2 as _pb2_79
import bilibili.broadcast.v1.broadcast_pb2 as _pb2_80
import bilibili.broadcast.v1.laser_pb2 as _pb2_81
import bilibili.broadcast.v1.mod_pb2 as _pb2_82
import bilibili.broadcast.v1.push_pb2 as _pb2_83
import bilibili.broadcast.v1.room_pb2 as _pb2_84
import bilibili.broadcast.v1.test_pb2 as _pb2_85
import bilibili.broadcast.v2.laser_pb2 as _pb2_86
import bilibili.cheese.gateway.player.v1.playurl_pb2 as _pb2_87
import bilibili.community.interfacess.biligram.v1.biligram_pb2 as _pb2_88
import bilibili.community.service.cert.v1.cert_pb2 as _pb2_89
import bilibili.community.service.dm.v1.dm_pb2 as _pb2_90
import bilibili.community.service.govern.v1.govern_pb2 as _pb2_91
import bilibili.dagw.component.avatar.common.common_pb2 as _pb2_92
import bilibili.dagw.component.avatar.v1.avatar_pb2 as _pb2_93
import bilibili.dagw.component.avatar.v1.plugin_pb2 as _pb2_94
import bilibili.dynamic.common.dynamic_pb2 as _pb2_95
import bilibili.dynamic.gw.gateway_pb2 as _pb2_96
import bilibili.dynamic.interfaces.campus.v1.api_pb2 as _pb2_97
import bilibili.dynamic.interfaces.feed.v1.api_pb2 as _pb2_98
import bilibili.gaia.gw.gw_api_pb2 as _pb2_99
import bilibili.im.interfaces.inner_interface.v1.api_pb2 as _pb2_100
import bilibili.im.interfaces.v1.im_pb2 as _pb2_101
import bilibili.im.type.im_pb2 as _pb2_102
import bilibili.live.app.room.v1.room_pb2 as _pb2_103
import bilibili.live.general.interfaces.v1.interfaces_pb2 as _pb2_104
import bilibili.main.common.arch.doll.v1.doll_pb2 as _pb2_105
import bilibili.main.community.reply.v1.reply_pb2 as _pb2_106
import bilibili.metadata.device.device_pb2 as _pb2_107
import bilibili.metadata.fawkes.fawkes_pb2 as _pb2_108
import bilibili.metadata.locale.locale_pb2 as _pb2_109
import bilibili.metadata.metadata_pb2 as _pb2_110
import bilibili.metadata.network.network_pb2 as _pb2_111
import bilibili.metadata.parabox.parabox_pb2 as _pb2_112
import bilibili.metadata.restriction.restriction_pb2 as _pb2_113
import bilibili.pagination.pagination_pb2 as _pb2_114
import bilibili.pangu.gallery.v1.gallery_pb2 as _pb2_115
import bilibili.pangu.gallery.v1.openplatform.apiserver.v1alpha1.api_pb2 as _pb2_116
import bilibili.pgc.gateway.player.v1.playurl_pb2 as _pb2_117
import bilibili.pgc.gateway.player.v2.playurl_pb2 as _pb2_118
import bilibili.pgc.service.premiere.v1.premiere_pb2 as _pb2_119
import bilibili.playershared.playershared_pb2 as _pb2_120
import bilibili.polymer.app.search.v1.search_pb2 as _pb2_121
import bilibili.polymer.community.govern.v1.govern_pb2 as _pb2_122
import bilibili.polymer.contract.v1.contract_pb2 as _pb2_123
import bilibili.polymer.demo.demo_pb2 as _pb2_124
import bilibili.polymer.list.v1.list_pb2 as _pb2_125
import bilibili.relation.interfaces.api_pb2 as _pb2_126
import bilibili.render.render_pb2 as _pb2_127
import bilibili.rpc.status_pb2 as _pb2_128
import bilibili.tv.interfaces.dm.v1.dm_pb2 as _pb2_129
import bilibili.vas.garb.model.sailing_pb2 as _pb2_130
import bilibili.vas.garb.service.card_pb2 as _pb2_131
import bilibili.vega.deneb.v1.deneb_pb2 as _pb2_132
import bilibili.web.interfaces.v1.interfaces_pb2 as _pb2_133
import bilibili.web.space.v1.space_pb2 as _pb2_134
import datacenter.hakase.protobuf.android_device_info_pb2 as _pb2_135
import pgc.biz.room_pb2 as _pb2_136
import pgc.gateway.vega.v1.vega_pb2 as _pb2_137

_MODULES: tuple[ModuleType, ...] = (
    _pb2_0,
    _pb2_1,
    _pb2_2,
    _pb2_3,
    _pb2_4,
    _pb2_5,
    _pb2_6,
    _pb2_7,
    _pb2_8,
    _pb2_9,
    _pb2_10,
    _pb2_11,
    _pb2_12,
    _pb2_13,
    _pb2_14,
    _pb2_15,
    _pb2_16,
    _pb2_17,
    _pb2_18,
    _pb2_19,
    _pb2_20,
    _pb2_21,
    _pb2_22,
    _pb2_23,
    _pb2_24,
    _pb2_25,
    _pb2_26,
    _pb2_27,
    _pb2_28,
    _pb2_29,
    _pb2_30,
    _pb2_31,
    _pb2_32,
    _pb2_33,
    _pb2_34,
    _pb2_35,
    _pb2_36,
    _pb2_37,
    _pb2_38,
    _pb2_39,
    _pb2_40,
    _pb2_41,
    _pb2_42,
    _pb2_43,
    _pb2_44,
    _pb2_45,
    _pb2_46,
    _pb2_47,
    _pb2_48,
    _pb2_49,
    _pb2_50,
    _pb2_51,
    _pb2_52,
    _pb2_53,
    _pb2_54,
    _pb2_55,
    _pb2_56,
    _pb2_57,
    _pb2_58,
    _pb2_59,
    _pb2_60,
    _pb2_61,
    _pb2_62,
    _pb2_63,
    _pb2_64,
    _pb2_65,
    _pb2_66,
    _pb2_67,
    _pb2_68,
    _pb2_69,
    _pb2_70,
    _pb2_71,
    _pb2_72,
    _pb2_73,
    _pb2_74,
    _pb2_75,
    _pb2_76,
    _pb2_77,
    _pb2_78,
    _pb2_79,
    _pb2_80,
    _pb2_81,
    _pb2_82,
    _pb2_83,
    _pb2_84,
    _pb2_85,
    _pb2_86,
    _pb2_87,
    _pb2_88,
    _pb2_89,
    _pb2_90,
    _pb2_91,
    _pb2_92,
    _pb2_93,
    _pb2_94,
    _pb2_95,
    _pb2_96,
    _pb2_97,
    _pb2_98,
    _pb2_99,
    _pb2_100,
    _pb2_101,
    _pb2_102,
    _pb2_103,
    _pb2_104,
    _pb2_105,
    _pb2_106,
    _pb2_107,
    _pb2_108,
    _pb2_109,
    _pb2_110,
    _pb2_111,
    _pb2_112,
    _pb2_113,
    _pb2_114,
    _pb2_115,
    _pb2_116,
    _pb2_117,
    _pb2_118,
    _pb2_119,
    _pb2_120,
    _pb2_121,
    _pb2_122,
    _pb2_123,
    _pb2_124,
    _pb2_125,
    _pb2_126,
    _pb2_127,
    _pb2_128,
    _pb2_129,
    _pb2_130,
    _pb2_131,
    _pb2_132,
    _pb2_133,
    _pb2_134,
    _pb2_135,
    _pb2_136,
    _pb2_137,
)


@dataclass(frozen=True, slots=True)
class ProtoMethod:
    """一个 gRPC 方法及其请求/响应消息类。"""

    service: str
    method: str
    request_cls: type[Message]
    response_cls: type[Message]

    @property
    def qualified_name(self) -> str:
        return f"{self.service}/{self.method}"


def _iter_message_descriptors(descriptor: Descriptor) -> Iterator[Descriptor]:
    """深度优先遍历消息描述符（含嵌套消息）。"""
    yield descriptor
    for nested in descriptor.nested_types:
        yield from _iter_message_descriptors(nested)


def _build_method_index() -> dict[str, tuple[ProtoMethod, ...]]:
    """构建「方法名 -> 该方法的所有候选定义」索引（同名方法可能属于多个服务）。"""
    index: dict[str, list[ProtoMethod]] = {}
    for module in _MODULES:
        for service in module.DESCRIPTOR.services_by_name.values():
            for method in service.methods:
                proto_method = ProtoMethod(
                    service=service.full_name,
                    method=method.name,
                    request_cls=GetMessageClass(method.input_type),
                    response_cls=GetMessageClass(method.output_type),
                )
                index.setdefault(method.name, []).append(proto_method)
    return {name: tuple(items) for name, items in index.items()}


def _build_message_index() -> dict[str, type[Message]]:
    """构建「protobuf 全限定名 -> 消息类」索引。"""
    index: dict[str, type[Message]] = {}
    for module in _MODULES:
        for descriptor in module.DESCRIPTOR.message_types_by_name.values():
            for nested in _iter_message_descriptors(descriptor):
                index.setdefault(nested.full_name, GetMessageClass(nested))
    return index


METHOD_INDEX: dict[str, tuple[ProtoMethod, ...]] = _build_method_index()
MESSAGE_INDEX: dict[str, type[Message]] = _build_message_index()
