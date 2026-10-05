class TouTiaoDb:
    SpaceFeedDataDb = rf"sqlite:////ToutiaoDb/SpaceFeedData.db?check_same_thread=False"
    AIO_SpaceFeedDataDb = (
        rf"sqlite+aiosqlite:////ToutiaoDb/SpaceFeedData.db?check_same_thread=False"
    )


class CONFIG:
    DBSetting = TouTiaoDb()
    root_dir = "//"
