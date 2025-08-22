@echo off
setlocal

echo 正在删除所有 __pycache__ 文件夹...

for /d /r . %%d in (__pycache__) do (
    echo 正在删除: "%%d"
    rmdir /s /q "%%d"
)

echo.
echo 清理完成。

endlocal
pause