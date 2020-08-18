all: clean
	mkdir bin
	javac -classpath lib/protobuf-java-3.7.1.jar -d bin/ java/Controller.java java/Branch.java java/Bank.java

clean:
	rm -rf bin/
