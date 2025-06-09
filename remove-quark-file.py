import os
import sys
import asyncio
import aiohttp
import aiomysql
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from config import settings
from logger import Logger

# 初始化日志
logger = Logger("quark_remover").logger

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

# 删除配置
BATCH_SIZE = int(settings.BATCH_SIZE)  # 每次处理的文件数量
SLEEP_TIME = int(settings.SLEEP_TIME)  # 每次循环后休眠时间(秒)
MAX_DB_RETRIES = 3  # 数据库操作最大重试次数
DB_RETRY_DELAY = 5  # 数据库重试延迟（秒）

class DatabaseError(Exception):
    """数据库操作异常"""
    pass

class RemoveError(Exception):
    """删除文件操作异常"""
    pass

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

    async def get_unique_directories(self) -> List[str]:
        """获取去重后的三级目录"""
        async def _operation():
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        SELECT DISTINCT 
                            SUBSTRING_INDEX(
                                SUBSTRING_INDEX(path, '/', 4),
                                '/',
                                4
                            ) as dir_path
                        FROM files 
                        WHERE path LIKE '/material/%'
                        AND is_processed = 0
                    ''')
                    results = await cur.fetchall()
                    return [result[0] for result in results if result[0]]
        return await self.execute_with_retry(_operation)

class QuarkRemover:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            "Authorization": settings.API_TOKEN,
            "Content-Type": "application/json"
        }

    async def remove_directory(self, dir_path: str) -> bool:
        """删除指定目录"""
        try:
            url = f"{settings.API_BASE_URL}/api/fs/remove"
            data = {
                "names": [""],  # 空字符串数组可以删除整个目录
                "dir": dir_path
            }
            
            async with self.session.post(url, json=data, headers=self.headers) as response:
                result = await response.json()
                if result.get('code') != 200:
                    raise RemoveError(f"删除失败: {result.get('message', '未知错误')}")
                
                logger.info("目录删除成功", extra={"dir_path": dir_path})
                return True

        except Exception as e:
            logger.error("删除目录时出错", extra={
                "dir_path": dir_path,
                "error": str(e)
            })
            raise RemoveError(f"删除目录失败: {str(e)}")

    async def create_directory(self, path: str) -> bool:
        """创建新目录"""
        try:
            url = f"{settings.API_BASE_URL}/api/fs/mkdir"
            data = {
                "path": path
            }
            
            async with self.session.post(url, json=data, headers=self.headers) as response:
                result = await response.json()
                if result.get('code') != 200:
                    raise RemoveError(f"创建目录失败: {result.get('message', '未知错误')}")
                
                logger.info("目录创建成功", extra={"path": path})
                return True

        except Exception as e:
            logger.error("创建目录时出错", extra={
                "path": path,
                "error": str(e)
            })
            raise RemoveError(f"创建目录失败: {str(e)}")

async def main():
    # 初始化数据库
    db = Database()
    await db.init_db()
    
    try:
        # 确保数据库连接可用
        await db.ensure_connected()
        
        async with aiohttp.ClientSession() as session:
            remover = QuarkRemover(session)
            
            # 获取未处理的目录
            try:
                directories = await db.get_unique_directories()
                if not directories:
                    logger.info("没有需要处理的目录")
                    return

                logger.info("开始处理目录", extra={"count": len(directories)})
                print(directories)
                exit()
                # 删除所有目录
                logger.info("开始删除目录...")
                for dir_path in directories:
                    try:
                        success = await remover.remove_directory(dir_path)
                    except Exception as e:
                        logger.error("处理目录出错", extra={"dir_path": dir_path, "error": str(e)})
                
                logger.info("所有目录删除完成")

                # 重新创建所有目录
                logger.info("开始创建目录...")
                for dir_path in directories:
                    try:
                        await remover.create_directory(dir_path)
                        logger.info("目录创建完成", extra={"dir_path": dir_path})
                    except Exception as e:
                        logger.error("创建目录失败", extra={"dir_path": dir_path, "error": str(e)})
                
                logger.info("所有目录创建完成")
                    
            except DatabaseError as e:
                logger.error("数据库操作失败", extra={"error": str(e)})

    except KeyboardInterrupt:
        logger.info("收到中断信号，程序退出")
    except Exception as e:
        logger.error("程序执行出错", extra={"error": str(e)})
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
