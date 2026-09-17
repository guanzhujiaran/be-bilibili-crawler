import os
from urllib.parse import urlencode
import execjs


class ToutiaoDecrypt:
    def __init__(self):
        _current_file_dir = os.path.dirname(os.path.abspath(__file__))
        with open(_current_file_dir + '/Toutiao_a_bogus.js', 'r',
                               encoding='utf-8') as f:

            _a_bogus_js_raw = f.read()
        # cwd 只决定 node 进程的相对路径起点；JS 里的 require('jsdom') 由 node 解析，
        # 命中容器里的 NODE_PATH=/opt/node_modules（见 Dockerfile.mono）。
        # 原先指向的 .../Enc/node_modules 目录并不存在，Popen(cwd=...) 会直接抛
        # FileNotFoundError，故改为脚本所在目录。
        self._a_bogus_gen = execjs.compile(_a_bogus_js_raw, cwd=_current_file_dir)

    def gen_abogus(self, params, ua) -> str:
        a_bogus = self._a_bogus_gen.call('get_a_bogus', urlencode(params), ua)
        return a_bogus


if __name__ == '__main__':
    ___ = ToutiaoDecrypt()
    ab = ___.gen_abogus({"114": "514"}, '11514:1919810')
    print(ab)
