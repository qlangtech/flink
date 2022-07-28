#mvn compile install -Dfast -Dscala-2.12 -DskipTests=true -DaltDeploymentRepository=base::default::http://localhost:8080/release

mvn compile deploy -Pscala-2.11 -Dfast -DskipTests=true -DaltDeploymentRepository=base::default::http://localhost:8080/release
