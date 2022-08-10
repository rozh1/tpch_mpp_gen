SET count=8

for /l %%x in (1, 1, %count%) do (
	start "" "dbgen.exe" -s 100 -C %count% -S %%x
)
pause