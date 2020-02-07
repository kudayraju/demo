title @optimus-common
call cls
call mvn clean dependency:resolve
call mvn clean compile
call mvn clean eclipse:eclipse
call mvn clean package
call mvn clean install