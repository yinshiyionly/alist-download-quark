import os
import sys
import asyncio
import aiohttp
import aiomysql
import shutil
import time
from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
from config import settings
from logger import Logger

# 初始化日志
logger = Logger("downloader").logger

# MySQL配置
MYSQL_CONFIG = {
    'host': settings.MYSQL_HOST,
    'port': settings.MYSQL_PORT,
    'user': settings.MYSQL_USER,
    'password': settings.MYSQL_ROOT_PASSWORD,  # 使用root密码
    'db': settings.MYSQL_DATABASE,
    'charset': 'utf8mb4',
    'autocommit': True  # 启用自动提交
}

# 下载配置
BATCH_SIZE = int(settings.BATCH_SIZE)  # 每次处理的文件数量
SLEEP_TIME = int(settings.SLEEP_TIME)  # 每次循环后休眠时间(秒)
DELETE_AFTER_DOWNLOAD = True  # 下载成功后是否删除文件
MIN_DISK_SPACE = int(settings.DISK_FREE)  # 最小磁盘空间要求(10GB)
REMOVE_PREFIX = settings.GET_ROOT_DIR  # 需要移除的路径前缀
OUT_PATH = settings.VIDEO_TMP_DIR  # 下载文件的根目录
MAX_DB_RETRIES = 3  # 数据库操作最大重试次数
DB_RETRY_DELAY = 5  # 数据库重试延迟（秒）

class DownloadError(Exception):
    """下载错误的自定义异常"""
    pass

class DatabaseError(Exception):
    """数据库错误的自定义异常"""
    pass

def check_disk_space(path: str, required_space: int = MIN_DISK_SPACE) -> bool:
    """检查磁盘空间是否足够"""
    try:
        total, used, free = shutil.disk_usage(path)
        logger.info(
            "检查磁盘空间", 
            extra={
                "total_gb": f"{total/1024/1024/1024:.2f}GB",
                "used_gb": f"{used/1024/1024/1024:.2f}GB",
                "free_gb": f"{free/1024/1024/1024:.2f}GB"
            }
        )
        return free > required_space
    except Exception as e:
        logger.error(f"检查磁盘空间失败", extra={"error": str(e)})
        return False

class Database:
    def __init__(self):
        self.pool = None

    async def ensure_connected(self):
        """确保数据库连接可用"""
        if self.pool is None or self.pool._closed:
            await self.init_db()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
        except Exception as e:
            logger.warning("数据库连接检查失败，尝试重新连接", extra={"error": str(e)})
            await self.close()
            await self.init_db()

    async def init_db(self):
        """初始化数据库连接池"""
        retries = 0
        last_error = None
        
        while retries < MAX_DB_RETRIES:
            try:
                if self.pool:
                    self.pool.close()
                    await self.pool.wait_closed()
                
                self.pool = await aiomysql.create_pool(**MYSQL_CONFIG)
                logger.info("数据库连接池初始化成功")
                return
            except Exception as e:
                last_error = e
                retries += 1
                logger.error("数据库连接失败", extra={
                    "retry": f"{retries}/{MAX_DB_RETRIES}",
                    "error": str(e)
                })
                if retries < MAX_DB_RETRIES:
                    await asyncio.sleep(DB_RETRY_DELAY * retries)
        
        raise DatabaseError(f"数据库连接失败，已达到最大重试次数: {last_error}")

    async def close(self):
        """关闭数据库连接池"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def execute_with_retry(self, operation):
        """使用重试机制执行数据库操作"""
        retries = 0
        last_error = None
        
        while retries < MAX_DB_RETRIES:
            try:
                await self.ensure_connected()
                return await operation()
            except Exception as e:
                last_error = e
                retries += 1
                logger.error("数据库操作失败", extra={
                    "retry": f"{retries}/{MAX_DB_RETRIES}",
                    "error": str(e)
                })
                if retries < MAX_DB_RETRIES:
                    await asyncio.sleep(DB_RETRY_DELAY * retries)
                    continue
                raise DatabaseError(f"数据库操作失败，已达到最大重试次数: {last_error}")

    async def get_unprocessed_files(self, limit: int = 10):
        """获取未处理的文件记录"""
        async def _operation():
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute('''
                        SELECT id, path, sign 
                        FROM files 
                        WHERE is_processed = 0 
                        LIMIT %s
                    ''', (limit,))
                    return await cur.fetchall()
        return await self.execute_with_retry(_operation)

    async def update_file_status(self, file_id: int, status: int, error_msg: str = None):
        """更新文件处理状态"""
        async def _operation():
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if error_msg:
                        await cur.execute('''
                            UPDATE files 
                            SET is_processed = %s, error_message = %s, 
                                updated_at = CURRENT_TIMESTAMP 
                            WHERE id = %s
                        ''', (status, error_msg, file_id))
                    else:
                        await cur.execute('''
                            UPDATE files 
                            SET is_processed = %s, updated_at = CURRENT_TIMESTAMP 
                            WHERE id = %s
                        ''', (status, file_id))
                    await conn.commit()
        await self.execute_with_retry(_operation)

class Downloader:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    def _get_download_path(self, file_path: str) -> tuple[str, str, str]:
        """
        处理下载路径和文件名
        返回: (目标目录, 临时文件名, 最终文件名)
        """
        # 移除前缀
        if file_path.startswith(REMOVE_PREFIX):
            file_path = file_path[len(REMOVE_PREFIX):]

        # 获取文件扩展名
        file_ext = os.path.splitext(file_path)[1]
        
        # 构建目标路径
        full_path = os.path.join(OUT_PATH, file_path.lstrip('/'))
        target_dir = os.path.dirname(full_path)
        final_filename = os.path.basename(full_path)
        temp_filename = f"{os.path.splitext(final_filename)[0]}.downloading"

        return target_dir, temp_filename, final_filename

    async def _get_file_size(self, url: str) -> Optional[int]:
        """获取远程文件大小"""
        try:
            async with self.session.head(url) as response:
                if response.status == 200:
                    return int(response.headers.get('Content-Length', 0))
        except Exception as e:
            logger.error("获取文件大小失败", extra={"error": str(e)})
        return None

    async def download_file(self, file_path: str, sign: str) -> bool:
        """
        下载文件
        返回: 是否下载成功
        """
        target_dir, temp_filename, final_filename = self._get_download_path(file_path)
        
        # 创建目标目录
        os.makedirs(target_dir, exist_ok=True)
        
        temp_file_path = os.path.join(target_dir, temp_filename)
        final_file_path = os.path.join(target_dir, final_filename)
        
        # 检查文件是否已存在
        if os.path.exists(final_file_path):
            logger.info("文件已存在", extra={"path": final_file_path})
            return True

        # 构建下载URL
        download_url = f"{settings.DOWNLOAD_HOST}{file_path}?sign={sign}"
        
        # 获取远程文件大小
        remote_size = None
        try:
            async with self.session.head(download_url) as response:
                if response.status == 200:
                    remote_size = int(response.headers.get('Content-Length', 0))
        except Exception as e:
            logger.error("获取文件大小失败", extra={"error": str(e)})
        
        # 如果获取到了文件大小，检查磁盘空间
        if remote_size and not check_disk_space(target_dir, remote_size):
            logger.error("磁盘空间不足", extra={"path": file_path, "size": remote_size})
            return False

        retries = 0
        while retries < 3:  # 最大重试次数
            try:
                # 获取已下载的文件大小
                local_size = os.path.getsize(temp_file_path) if os.path.exists(temp_file_path) else 0
                
                # 设置断点续传的header
                headers = {}
                if local_size > 0:
                    headers['Range'] = f'bytes={local_size}-'

                async with self.session.get(download_url, headers=headers) as response:
                    if response.status not in (200, 206):
                        raise DownloadError(f"下载失败，状态码: {response.status}")

                    # 以追加模式打开文件
                    mode = 'ab' if local_size > 0 else 'wb'
                    with open(temp_file_path, mode) as f:
                        async for chunk in response.content.iter_chunked(8192):
                            if chunk:
                                f.write(chunk)

                # 下载完成后重命名文件
                os.rename(temp_file_path, final_file_path)
                logger.info("文件下载完成", extra={"path": final_file_path})
                
                # 如果配置了下载后删除，则删除文件
                if DELETE_AFTER_DOWNLOAD:
                    try:
                        os.remove(final_file_path)
                        logger.info("文件已删除", extra={"path": final_file_path})
                    except Exception as e:
                        logger.error("删除文件失败", extra={"path": final_file_path, "error": str(e)})
                
                return True

            except Exception as e:
                retries += 1
                logger.error("下载失败", extra={
                    "retry": f"{retries}/3",
                    "path": file_path,
                    "error": str(e)
                })
                await asyncio.sleep(5 * retries)  # 指数退避
                
                # 检查临时文件是否完整
                if os.path.exists(temp_file_path):
                    if remote_size is None or os.path.getsize(temp_file_path) < remote_size:
                        # 文件不完整，下次继续下载
                        continue
                    else:
                        # 文件已完整下载，重命名并返回
                        os.rename(temp_file_path, final_file_path)
                        
                        # 如果配置了下载后删除，则删除文件
                        if DELETE_AFTER_DOWNLOAD:
                            try:
                                os.remove(final_file_path)
                                logger.info("文件已删除", extra={"path": final_file_path})
                            except Exception as e:
                                logger.error("删除文件失败", extra={"path": final_file_path, "error": str(e)})
                        
                        return True

        # 清理垃圾文件
        if os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.info("清理临时文件", extra={"path": temp_file_path})
            except Exception as e:
                logger.error("清理临时文件失败", extra={"path": temp_file_path, "error": str(e)})

        return False

async def main():
    db = Database()
    
    try:
        # 无限循环，支持用户中断
        while True:
            try:
                # 确保数据库连接可用
                await db.ensure_connected()
                
                # 检查磁盘空间是否满足最低要求
                if not check_disk_space(OUT_PATH):
                    logger.warning(f"磁盘空间不足{MIN_DISK_SPACE/1024/1024/1024:.2f}GB，休眠后重试")
                    await asyncio.sleep(SLEEP_TIME)
                    continue
                    
                async with aiohttp.ClientSession() as session:
                    downloader = Downloader(session)
                    
                    # 获取未处理的文件
                    try:
                        files = await db.get_unprocessed_files(limit=BATCH_SIZE)
                        if not files:
                            logger.info("没有更多未处理的文件，休眠后继续")
                            await asyncio.sleep(SLEEP_TIME)
                            continue

                        logger.info("开始处理文件", extra={"count": len(files)})
                        
                        for file in files:
                            try:
                                success = await downloader.download_file(file['path'], file['sign'])
                                if success:
                                    await db.update_file_status(file['id'], 1)
                                    logger.info("文件处理完成", extra={"path": file['path']})
                                else:
                                    error_msg = "下载失败，已达到最大重试次数"
                                    await db.update_file_status(file['id'], -1, error_msg)
                                    logger.error("文件处理失败", extra={"path": file['path'], "error": error_msg})
                            except Exception as e:
                                logger.error("处理文件出错", extra={"path": file['path'], "error": str(e)})
                                await db.update_file_status(file['id'], -1, str(e))
                    except DatabaseError as e:
                        logger.error("数据库操作失败", extra={"error": str(e)})
                        await asyncio.sleep(SLEEP_TIME)
                        continue
                    
                    logger.info(f"本次循环完成，休眠{SLEEP_TIME}秒")
                    await asyncio.sleep(SLEEP_TIME)
            
            except Exception as e:
                logger.error("循环执行出错", extra={"error": str(e)})
                await asyncio.sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        logger.info("收到中断信号，程序退出")
    except Exception as e:
        logger.error("程序执行出错", extra={"error": str(e)})
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())