# /bin/sh
echo -e "Starting Post Processing ..."
echo -e "Start notebook processing - template.ipynb"
sed 's/BASEDIR_TEMP/profile\/'$1'/g' template.ipynb > tpcxx_$1_$2.ipynb
sed -i 's/APPID_TEMP/'$2'/g' tpcxx_$1_$2.ipynb
if [ $# -eq 4 ]
then
	sed -i 's/LAST_BASEDIR/profile\/'$3'/g' tpcxx_$1_$2.ipynb
	sed -i 's/LAST_APPID/'$4'/g' tpcxx_$1_$2.ipynb
fi

hadoop fs -mkdir /history
hadoop fs -cp /profile/$1/$2/app.log /history/$2
echo -e "Finish notebook processing - template.ipynb"

echo -e "Start notebook execution - tpcxx.ipynb"
mkdir -p html
jupyter nbconvert --execute --to notebook --inplace --allow-errors --ExecutePreprocessor.timeout=-1 ./tpcxx_$1_$2.ipynb --template classic
jupyter nbconvert --to html ./tpcxx_$1_$2.ipynb --output html/tpcxx_$1_$2.html --template classic

#echo -e "notebook processing - tpch_summary.ipynb"
#sed 's/BASEDIR_TEMP/profile\/'$1'/g' tpch_template_summary.ipynb > tpch_summary_$1_$2.ipynb
#sed -i 's/APPID_TEMP/'$2'/g' tpch_summary_$1_$2.ipynb
#if [ $# -eq 4 ]
#then
#	sed -i 's/LAST_BASEDIR/profile\/'$3'/g' tpch_summary_$1_$2.ipynb
#	sed -i 's/LAST_APPID/'$4'/g' tpch_summary_$1_$2.ipynb
#fi

#jupyter nbconvert --execute --to notebook --inplace --allow-errors --ExecutePreprocessor.timeout=-1 ./tpch_summary_$1_$2.ipynb --template classic
#jupyter nbconvert --to html  --no-input ./tpch_summary_$1_$2.ipynb --output html/tpch_summary_$1_$2.html --template classic
#echo -e "Finish notebook processing - tpch.ipynb"

#rm -rf ./tpch_summary_$1_$2.ipynb
echo -e "Finish Post Processing !!!"
