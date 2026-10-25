from pydantic import Field
from CONFIG import CONFIG, settings

from Models.base.custom_pydantic import CustomBaseModel
import os

_DEFAULT_BACKUP_RELATIVE_DIR = "scripts/database/dyn_backup"


def _get_zip_path() -> str:
    # 优先取配置项 DYN_BACKUP_DIR（容器里指向挂载出来的宿主机目录），
    # 未配置时回落到代码根目录下的默认归档目录。
    return settings.DYN_BACKUP_DIR or os.path.join(
        CONFIG.root_dir, _DEFAULT_BACKUP_RELATIVE_DIR
    )


class MainConf(CustomBaseModel):
    between_ts: list[int] | None = None
    is_gen_word_cloud: bool = False
    is_gen_zip: bool = False
    gen_zip_path: str = Field(default_factory=_get_zip_path)
    is_delete_generated_data: bool = False
