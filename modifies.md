## set version

sh ./setversion.sh

## package 

mvn compile package -DskipTests=true -Dfast -Dscala-2.11 

## deploy

mvn compile deploy -DskipTests=true -Dfast -Dscala-2.11 -DaltDeploymentRepository=base::default::http://localhost:8080/release




