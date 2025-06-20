from pydantic_settings import BaseSettings
from typing import Set


class Settings(BaseSettings):
    """应用程序配置类

    用于管理应用程序的所有配置项，包括基础设置、文件存储、音视频处理参数等。
    继承自pydantic.BaseSettings，支持从环境变量和.env文件加载配置。
    """

    # 基础配置
    APP_NAME: str = "download-to-local"  # 应用程序名称
    APP_HOST: str = "127.0.0.1" # 应用主机
    API_BASE_URL: str = ""
    API_TOKEN: str = ""
    DEBUG: bool = False  # 调试模式开关

    # 文件保存配置
    SAVE_ROOT_DIR: str  # 文件保存根目录
    PRESERVE_PATH_STRUCTURE: bool = True  # 是否保留原始路径结构
    
    # 磁盘预留空间-10GB
    DISK_FREE: int = 10 * 1024 * 1024 * 1024
    # 获取的根目录
    GET_ROOT_DIR: str = "/"
    # 下载业务的host
    DOWNLOAD_HOST: str
    GET_FILE_INFO_HOST: str
    # 每次处理的文件数量
    BATCH_SIZE: int = 5
    # 每次循环后休眠时间(秒)
    SLEEP_TIME: int = 60
    # 下载成功后删除
    DELETE_AFTER_DOWNLOAD: bool = True

    # MySQL配置
    MYSQL_HOST: str = "localhost"  # MySQL主机地址
    MYSQL_PORT: int = 3306  # MySQL端口
    MYSQL_DATABASE: str  # MySQL数据库名
    MYSQL_USER: str  # MySQL用户名
    MYSQL_PASSWORD: str  # MySQL密码
    MYSQL_ROOT_PASSWORD: str  # MySQL root密码

    # 日志配置
    LOG_LEVEL: str = "INFO"  # 日志级别
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # 日志格式
    LOG_DIR: str = "logs"  # 日志文件目录
    LOG_FILE_PREFIX: str = "app"  # 日志文件名前缀
    LOG_FILE_MAX_BYTES: int = 10 * 1024 * 1024  # 单个日志文件最大大小（10MB）
    LOG_FILE_BACKUP_COUNT: int = 5  # 日志文件备份数量

    class Config:
        """配置类设置

        case_sensitive: 区分大小写
        env_file: 环境变量文件路径
        """

        case_sensitive = True
        env_file = ".env"


settings = Settings()  # 创建全局配置实例