pip install -r requirements.txt
python setup.py build_ext --inplace
pyinstaller --onedir --noconfirm --nowindow "gendb.py" 
mkdir dist\tpch3_dist
copy /Y tpch3_dist dist\tpch3_dist\
copy /Y JBinaryTransformer.jar dist\
copy /Y run.bat dist\