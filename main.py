import os
import json
import asyncio
import aiohttp
import aiomysql
from typing import List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
from config import settings


# 加载环境变量
load_dotenv()

# 配置
API_BASE_URL = settings.API_BASE_URL
API_TOKEN= settings.API_TOKEN

# MySQL配置
MYSQL_CONFIG = {
    'host': settings.MYSQL_HOST,
    'port': settings.MYSQL_PORT,
    'user': settings.MYSQL_USER,
    'password': settings.MYSQL_PASSWORD,
    'db': settings.MYSQL_DATABASE,
    'charset': 'utf8mb4'
}

class AlistClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            "Authorization": API_TOKEN,
            "Content-Type": "application/json"
        }

    async def list_files(self, path: str, page: int = 1, per_page: int = 100) -> Dict[str, Any]:
        """获取指定路径下的文件列表"""
        url = f"{API_BASE_URL}/api/fs/list"
        data = {
            "path": path,
            "password": "",
            "page": page,
            "per_page": per_page,
            "refresh": False
        }
        
        async with self.session.post(url, json=data, headers=self.headers) as response:
            return await response.json()

class Database:
    def __init__(self):
        self.pool = None

    async def init_db(self):
        """初始化数据库连接池"""
        self.pool = await aiomysql.create_pool(**MYSQL_CONFIG)

    async def close(self):
        """关闭数据库连接池"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    async def file_exists(self, full_path: str) -> bool:
        """检查文件是否已存在于数据库中"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    'SELECT 1 FROM files WHERE path = %s',
                    (full_path,)
                )
                result = await cur.fetchone()
                return bool(result)

    async def save_file_info(self, file_info: Dict[str, Any], full_path: str):
        """保存文件信息到数据库"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                # 获取文件名和目录路径
                file_name = file_info['name']
                modified_time = datetime.fromisoformat(file_info['modified'].replace('Z', '+00:00'))
                
                await cur.execute('''
                    INSERT INTO files (name, path, size, sign, is_processed)
                    VALUES (%s, %s, %s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE
                    size = VALUES(size),
                    sign = VALUES(sign)
                ''', (
                    file_name,
                    full_path,
                    file_info['size'],
                    modified_time,
                    file_info['sign']
                ))
                await conn.commit()

async def process_directory(client: AlistClient, db: Database, path: str):
    """递归处理目录"""
    page = 1
    while True:
        response = await client.list_files(path, page)
        
        if response['code'] != 200:
            print(f"Error fetching path {path}: {response['message']}")
            break

        data = response['data']
        content = data['content']
        
        if not content:
            break

        # 处理当前页的所有文件和目录
        for item in content:
            full_path = os.path.join(path, item['name']).replace('\\', '/')
            
            if item['is_dir']:
                # 如果是目录，递归处理
                await process_directory(client, db, full_path)
            else:
                # 如果是文件，检查是否已处理过
                if not await db.file_exists(full_path):
                    await db.save_file_info(item, full_path)
                    print(f"Added new file: {full_path}")

        # 检查是否还有下一页
        total_pages = (data['total'] + 99) // 100  # 每页100项
        if page >= total_pages:
            break
        
        page += 1

async def main():
    # 初始化数据库
    db = Database()
    await db.init_db()

    try:
        # 创建HTTP会话
        async with aiohttp.ClientSession() as session:
            client = AlistClient(session)
            
            # 从根目录开始处理
            root_path = settings.GET_ROOT_DIR
            await process_directory(client, db, root_path)
    finally:
        # 确保关闭数据库连接
        await db.close()

if __name__ == "__main__":
    asyncio.run(main()) 