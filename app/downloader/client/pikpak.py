import asyncio

from pikpakapi import PikPakApi, DownloadStatus

import log
from app.downloader.client._base import _IDownloadClient
from app.utils.types import DownloaderType
from config import Config

import os
import bencodepy
import httpx
import tempfile
import json
import logging
import hashlib

TOKEN_FILE = "/nastool/pikpak.json"

class PikPak(_IDownloadClient):
    
    schema = "pikpak"
    # 下载器ID
    client_id = "pikpak"
    client_type = DownloaderType.PIKPAK
    client_name = DownloaderType.PIKPAK.value
    _client_config = {}

    _client = None
    username = None
    password = None
    proxy = None

    def __init__(self, config=None):
        if config:
            self._client_config = config
        self.init_config()
        self.connect()

    #初始化配置
    def init_config(self):
        # 采用最新的api传递代理参数
        if self._client_config:
            self.username = self._client_config.get("username")
            self.password = self._client_config.get("password")
            self.proxy = self._client_config.get("proxy")

            httpx_client_args = {
                "transport": httpx.AsyncHTTPTransport(retries=3)
            }

            # 检查代理配置是否存在，并正确格式化
            if self.proxy:
                if not (self.proxy.startswith("http://") or self.proxy.startswith("https://") or self.proxy.startswith("socks5://")):
                    # 如果代理不是以http://、https://或socks5://开始，则默认为HTTP代理
                    self.proxy = "http://" + self.proxy
                httpx_client_args["proxies"] = self.proxy

            if self.username and self.password:
                self._client = PikPakApi(
                    username=self.username,
                    password=self.password,
                    httpx_client_args=httpx_client_args,
                    token_refresh_callback=self.save_token,
                )


    def save_token(self, client):
        """Token 刷新后自动保存到本地"""
        try:
            os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)  # 确保路径存在
            with open(TOKEN_FILE, "w") as f:
                json.dump(client.to_dict(), f, indent=4)
            logging.info("Token 已刷新并保存至本地")
        except Exception as e:
            logging.error(f"保存 Token 失败: {str(e)}")

    def load_token(self):
        """尝试从本地加载 Token"""
        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE, "r") as f:
                    data = json.load(f)
                self._client = PikPakApi.from_dict(data)
                logging.info("Token 加载成功，使用缓存登录")
                return True
            except json.JSONDecodeError:
                logging.error("Token 文件损坏，删除并重新登录")
                os.remove(TOKEN_FILE)  # 删除损坏的 Token 文件
            except Exception as e:
                logging.error(f"加载 Token 失败: {str(e)}")
        return False


    @classmethod
    def match(cls, ctype):
        return True if ctype in [cls.client_id, cls.client_type, cls.client_name] else False

    def connect(self):
        """尝试连接 PikPak 账户"""
        try:
            if not self.load_token():  # 优先加载 Token，若失败则重新登录
                if self._client is None:
                    logging.error("PikPak 客户端未初始化，无法连接！")
                    return
                asyncio.run(self._client.login())
                self.save_token(self._client)  # 登录成功后保存 Token
        except Exception as err:
            logging.error(f"PikPak 连接失败: {str(err)}")
            return

    def refresh_token(self):
        """手动刷新 Token"""
        try:
            if self._client is None:
                logging.error("PikPak 客户端未初始化，无法刷新 Token！")
                return
            asyncio.run(self._client.refresh_access_token())
            self.save_token(self._client)  # 刷新 Token 后存储
            logging.info("Token 刷新成功")
        except Exception as e:
            logging.error(f"Token 刷新失败: {str(e)}")

    def refresh_and_reconnect(self):
        """手动刷新 Token 并重新连接"""
        self.refresh_token()
        self.connect()


    def get_status(self):
        if not self._client:
            return False
        try:
            asyncio.run(self._client.login())
            if self._client.user_id is None:
                log.info("PikPak 登录失败")
                return False
        except Exception as err:
            log.error("PikPak 登录出错：%s" % str(err))
            return False

        return True

    def get_torrents(self, ids=None, status=None, **kwargs):
        rv = []
        if self._client.user_id is None:
            if self.get_status():
                return [], False

        if ids is not None:
            for id in ids:
                status = asyncio.run(self._client.get_task_status(id, ''))
                if status == DownloadStatus.downloading:
                    rv.append({"id": id, "finish": False})
                if status == DownloadStatus.done:
                    rv.append({"id": id, "finish": True})
        return rv, True

    def get_completed_torrents(self, **kwargs):
        return []

    def get_downloading_torrents(self, **kwargs):
        if self._client.user_id is None:
            if self.get_status():
                return []
        try:
            offline_list = asyncio.run(self._client.offline_list())
            return offline_list['tasks']
        except Exception as err:
            print(str(err))
            return []

    def get_transfer_task(self, **kwargs):
        pass

    def get_remove_torrents(self, **kwargs):
        return []

    # def add_torrent(self, content, download_dir=None, **kwargs):
    #     try:
    #         task = asyncio.run(self._client.offline_download(content, download_dir))
    #         taskId = task.get('task', {}).get('id')
    #         return taskId is not None and bool(taskId)
    #     except Exception as e:
    #         log.error("PikPak 添加离线下载任务失败: %s" % str(e))
    #         return None

    # 添加离线任务
    def add_torrent(self, content, download_dir=None, **kwargs):
        """
        添加离线任务，支持磁力链接、本地 .torrent 文件路径 和 HTTP 种子链接
        :param content: 磁力链接 / 种子文件路径 / 远程 .torrent 直链
        :param download_dir: 文件保存路径 (PikPak 上级文件夹 ID)
        """



        if not self._client:
            log.error("PikPak 客户端未初始化，无法添加离线任务！")
            return None

        # 记录 content 类型，方便调试
        log.info(f"add_torrent received content of type: {type(content)}")

        # **获取并打印 PikPak 目录**
        log.info("正在获取 PikPak 目录列表...")
        folder_list = self.list_pikpak_directories()
        if folder_list is None:
            # log.error("无法获取 PikPak 目录，可能是 API 连接问题！")
            return None
        # else:
            # log.info(f"PikPak 目录列表: {json.dumps(folder_list, indent=4, ensure_ascii=False)}")

        # **检查 download_dir 是否有效**
        if download_dir:
            folder_ids = [f["id"] for f in folder_list.get("files", [])]
            if download_dir not in folder_ids:
                log.error(f"指定的下载目录 ID '{download_dir}' 不存在，请检查！")
                return None

        # **检查任务是否已存在**
        existing_tasks = self.get_downloading_torrents()
        for task in existing_tasks:
            if task.get("name") == content or task.get("magnet_uri") == content:
                log.warning(f"任务已存在，不再重复添加: {content}")
                return False

        try:
            # 如果 content 是二进制数据（可能是 .torrent 文件内容）
            if isinstance(content, bytes):
                log.info("检测到 .torrent 文件的二进制数据，尝试转换为磁力链接")
                content = self.torrent_to_magnet(content)
                if not content:
                    log.error("磁力链接转换失败")
                    return None

            # 处理 HTTP/HTTPS 远程种子文件链接
            elif isinstance(content, str) and (content.startswith("http://") or content.startswith("https://")):
                log.info(f"检测到 HTTP 种子文件链接: {content}")
                torrent_file_path = self.download_torrent_file(content)
                if not torrent_file_path:
                    log.error("种子文件下载失败")
                    return None
                content = self.torrent_to_magnet(torrent_file_path)
                if not content:
                    log.error("磁力链接转换失败")
                    return None

            # 处理本地种子文件
            elif isinstance(content, str) and os.path.exists(content) and content.endswith(".torrent"):
                log.info(f"检测到本地种子文件: {content}")
                content = self.torrent_to_magnet(content)
                if not content:
                    log.error("磁力链接转换失败")
                    return None

            # 处理磁力链接
            if isinstance(content, str) and content.startswith("magnet:?xt="):
                log.info(f"最终提交的磁力链接: {content}")
                task = asyncio.run(self._client.offline_download(content, parent_id=download_dir))
            else:
                log.error(f"未知的下载链接格式: {content}")
                return None

            # 解析任务 ID
            task_id = task.get("task", {}).get("id") if isinstance(task, dict) else None
            if task_id:
                log.info(f"PikPak 任务添加成功，任务 ID: {task_id}")
                return True
            else:
                log.warning("PikPak 任务添加失败，未获取到任务 ID")
                return False

        except Exception as e:
            log.error(f"添加 PikPak 任务失败: {str(e)}")
            return None

    def download_torrent_file(self, url):
        """
        下载 .torrent 文件并保存到本地
        :param url: .torrent 文件的 HTTP/HTTPS 链接
        :return: 下载后的本地文件路径
        """
        try:
            response = httpx.get(url, timeout=10)
            if response.status_code == 200:
                file_path = f"/tmp/{os.path.basename(url)}"
                with open(file_path, "wb") as f:
                    f.write(response.content)
                log.info(f"种子文件下载成功: {file_path}")
                return file_path
            else:
                log.error(f"种子文件下载失败, HTTP 状态码: {response.status_code}")
                return None
        except Exception as e:
            log.error(f"种子文件下载异常: {str(e)}")
            return None

    def torrent_to_magnet(self, torrent_input):
        """
        将 .torrent 文件或其二进制数据转换为磁力链接
        :param torrent_input: .torrent 文件路径 或者 二进制内容
        :return: 磁力链接 或 None
        """
        try:
            # 读取 .torrent 文件内容
            if isinstance(torrent_input, str) and os.path.exists(torrent_input):
                with open(torrent_input, "rb") as f:
                    torrent_data = f.read()
            elif isinstance(torrent_input, bytes):
                torrent_data = torrent_input
            else:
                log.error("torrent_to_magnet: 输入数据格式错误")
                return None

            # 解析种子文件
            torrent_dict = bencodepy.decode(torrent_data)

            # 确保 "info" 字段存在
            if b'info' not in torrent_dict:
                log.error("解析错误: 种子文件缺少 'info' 字段")
                return None

            info = torrent_dict[b'info']

            # 计算 info_hash
            sha1 = hashlib.sha1()
            sha1.update(bencodepy.encode(info))
            info_hash = sha1.hexdigest()

            # 获取 Tracker 列表
            trackers = set()
            if b'announce' in torrent_dict:
                trackers.add(torrent_dict[b'announce'].decode())

            if b'announce-list' in torrent_dict:
                for tier in torrent_dict[b'announce-list']:
                    if isinstance(tier, list):
                        for url in tier:
                            trackers.add(url.decode())
                    else:
                        trackers.add(tier.decode())

            # 生成 Magnet 链接
            magnet_link = f'magnet:?xt=urn:btih:{info_hash}'
            for tracker in trackers:
                magnet_link += f'&tr={tracker}'

            log.info(f"生成的磁力链接: {magnet_link}")
            return magnet_link

        except Exception as e:
            log.error(f"torrent_to_magnet 解析错误: {str(e)}")
            return None

    # 需要完成
    def delete_torrents(self, delete_file, ids):
        pass

    def start_torrents(self, ids):
        pass

    def stop_torrents(self, ids):
        pass

    # 需要完成
    def set_torrents_status(self, ids, **kwargs):
        pass

    def get_download_dirs(self):
        return []

    def change_torrent(self, **kwargs):
        pass

    # 需要完成
    def get_downloading_progress(self, **kwargs):
        """
        获取正在下载的种子进度
        """
        Torrents = self.get_downloading_torrents()
        DispTorrents = []
        for torrent in Torrents:
            DispTorrents.append({
                'id': torrent.get('id'),
                'file_id': torrent.get('file_id'),
                'name': torrent.get('file_name'),
                'nomenu': True,
                'noprogress': True
            })
        return DispTorrents

    def set_speed_limit(self, **kwargs):
        """
        设置速度限制
        """
        pass

    def get_type(self):
        return self.client_type

    def get_files(self, tid):
        pass

    def recheck_torrents(self, ids):
        pass

    def set_torrents_tag(self, ids, tags):
        pass

    def list_pikpak_directories(self):
        """ 获取 PikPak 云端目录列表 (同步方式) """
        if not self._client:
            log.error("PikPak 客户端未初始化，无法获取目录列表！")
            return None

        try:
            folder_list = asyncio.run(self._client.file_list())  # 直接运行异步方法
            return folder_list
        except Exception as e:
            log.error(f"获取 PikPak 目录列表失败: {str(e)}")
            return None
