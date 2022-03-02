JDK = openjdk/jdk-13.0.1/bin
JDK_PATH = /home/miguelareias/.bin
TEST = MultiThreadBenchRunner
FFP = com/ffp

MultiThreadBenchRunnerAll:
	make GetUpdatedFFP
	$(JDK_PATH)/$(JDK)/java  -server $(TEST) > tmp/MAPS
	make clean

GetUpdatedFFP:
	cp ../$(FFP)/*.java com/ffp
	cd $(FFP) ; $(JDK_PATH)/$(JDK)/javac -XDignore.symbol.file *.java
	$(JDK_PATH)/$(JDK)/javac *.java

clean: 
	del *.class

