@echo off
setlocal

echo ����ɾ������ __pycache__ �ļ���...

for /d /r . %%d in (__pycache__) do (
    echo ����ɾ��: "%%d"
    rmdir /s /q "%%d"
)

echo.
echo ������ɡ�

endlocal
pause