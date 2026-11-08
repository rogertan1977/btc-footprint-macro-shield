@echo off
:: ==============================================================================
:: Zone-Based Footprint ^& Macro Validation Agent 启动脚本
:: ==============================================================================

:: 1. 强制控制台编码为 UTF-8，彻底杜绝中文乱码
chcp 65001 >nul

:: 2. 设置控制台窗口标题（使用 ^& 转义，防止 Windows 将 Macro 误判为系统命令）
title Zone-Based Footprint ^& Macro Validation Agent [Running]

:: 3. 自动检测系统是否已正确配置 Python 环境变量
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo [错误] 未检测到 Python 环境！
    echo 请确认 Python 已正确安装并勾选了 "Add Python to PATH"。
    echo.
    pause
    exit /b 1
)

:: 4. 输出整洁的启动终端横幅（所有 & 符号均已添加 ^ 转义）
cls
echo ==============================================================================
echo   Zone-Based Footprint ^& Macro Validation Agent
echo   伏击圈足迹与宏观护盾狙击手
echo ==============================================================================
echo [*] 正在拉起 Python 异步计算引擎 (agent.py)...
echo.

:: 5. 启动主计算引擎
python agent.py

:: 6. 异常退出或手动终止时保持窗口，便于查看回溯日志
echo.
echo ==============================================================================
echo [*] 引擎进程已结束退出。
echo ==============================================================================
pause