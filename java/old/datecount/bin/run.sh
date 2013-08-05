hadoop dfs -rmr /user/ahanna/jan25out/
hadoop jar datecount.jar org.ahanna.DateCount jan25/jan25.json /user/ahanna/jan25out/ \
-patternFiles jan25dicts/alexandria.txt,jan25dicts/assuit.txt,jan25dicts/cairo.txt,jan25dicts/damietta.txt,jan25dicts/giza.txt,jan25dicts/mahalla.txt,jan25dicts/mansoura.txt,jan25dicts/minya.txt,jan25dicts/portsaid.txt,jan25dicts/qena.txt,jan25dicts/sharm.txt,jan25dicts/sinai.txt,jan25dicts/suez.txt,jan25dicts/tanta.txt
