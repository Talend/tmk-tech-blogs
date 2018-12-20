env.PROJECT_GIT_NAME = 'TALEND_JOB_PIPELINE'
env.PROJECT_NAME = env.PROJECT_GIT_NAME.toLowerCase()
env.JOB = 'test'
env.VERSION = '0.1'
env.GIT_URL = 'https://github.com/tgourdel/talend-pipeline-job.git'
env.TYPE = "" // if big data = _mr
env.DOCKERHUB_USER = "talendinc"
env.IMAGE_NAME = "dockerimagename"

// Credentials IDs (Manage Jenkins => Credentials)
env.GIT_CREDENTIALS_ID = 'github'

node {
 	// Clean workspace before doing anything
    try {
        def userInput
        def deployprod
        stage('Initialize') {
            sh '''
                echo "PATH = ${PATH}"
                echo "M2_HOME = ${M2_HOME}"
            ''' 
        }
        stage ('Git Checkout') {
            git(
                url: "${GIT_URL}",
                credentialsId: "${GIT_CREDENTIALS_ID}",
                branch: 'master'
            )       
            mvnHome = tool 'M3'
        }
        stage ('Build, Test and Publish Jobs to Nexus') {
                    withMaven(
                            // Maven installation declared in the Jenkins "Global Tool Configuration"
                            maven: 'M3',
                            // Maven settings.xml file defined with the Jenkins Config File Provider Plugin
                            // Maven settings and global settings can also be defined in Jenkins Global Tools Configuration
                            mavenSettingsConfig: 'maven-file',
                            mavenOpts: '-Dproduct.path=/cmdline -Dgeneration.type=local -DaltDeploymentRepository=snapshots::default::http://nexus:8081/repository/snapshots/ -Xms1024m -Xmx3096m') 
                            {
                    
                        // Run the maven build
                        sh "mvn -f $PROJECT_GIT_NAME/poms/pom.xml clean deploy -fn -e -pl jobs/process${TYPE}/${JOB}_${VERSION} -am"
                    
                        }    
        }
        stage ('Build Job and Publish Docker Image') {

            withCredentials([usernamePassword(credentialsId: 'DockerRegistry', usernameVariable: 'Dusername', passwordVariable: 'Dpassword')]) {
                withMaven(
                    maven: 'M3', 
                    mavenSettingsConfig: 'maven-file',
                    mavenOpts: "-Dgeneration.type=local -Dproduct.path=/cmdline -Ddocker.push.registry=registry.hub.docker.com/${env.Dusername} -Ddocker.push.username=${env.Dusername} -Ddocker.push.password=${env.Dpassword} -Xms1024m -Xmx3096m") {
                
                   sh "mvn -f $PROJECT_GIT_NAME/poms/pom.xml -Pdocker deploy -e -pl jobs/process${jobtobuild} -am -Dtalend.docker.name=${env.IMAGE_NAME}"
                }
            }
            sleep(5)
        }
        stage ('Deployment environment ?') {
          userInput = input(id: 'userInput',    
                                  message: 'Deployment environment',    
                                  parameters: [
                                    [$class: 'ChoiceParameterDefinition', choices: "AWS Fargate\nAzure ACI", name: 'Env']
                                         ]  
                )
        }
        stage ('Serverless deployment') {

            if (userInput == "AWS Fargate"){
                sh "echo 'AWS Fargate'"
                
                withCredentials([usernamePassword(credentialsId: 'AWS', usernameVariable: 'ACCESS_KEY_ID', passwordVariable: 'SECRET_ACCESS_KEY')]) {
                    withDockerContainer('aws-cli') {

                        sh"""
                        aws configure set aws_access_key_id ${env.ACCESS_KEY_ID}
                        aws configure set aws_secret_access_key ${env.SECRET_ACCESS_KEY}
                        aws configure set default.region us-east-1
                        aws ecs run-task --cluster TalendDeployedPipeline --task-definition TalendContainerizedJob --network-configuration awsvpcConfiguration={subnets=[subnet-6b30d745],securityGroups=[],assignPublicIp=ENABLED} --launch-type FARGATE
                        """
                    }
                }
            }
            else if (userInput == "Azure ACI"){
                sh "echo 'Azure ACI'"
                
                withCredentials([usernamePassword(credentialsId: 'azure', usernameVariable: 'username', passwordVariable: 'password')]) {
                    withDockerContainer('azure-cli') {

                        def now = System.currentTimeMillis()    

                        sh"""
                        az login -u ${env.username} -p ${env.password}
                        az container create --resource-group talend-job-serverless --name talendjob${now} --image talendinc/job:0.1  --restart-policy OnFailure
                        """
                    }
                }
                
            }
            else {
                  sh "echo 'nothing'"
            }
 
        }
    } catch (err) {
        currentBuild.result = 'FAILED'
        throw err
    }
}
