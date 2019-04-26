import org.apache.spark.SparkConf

import org.apache.http.entity.StringEntity
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient

import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets

import net.liftweb.json._

/**
  * Application that outputs Spark Mesos Secret Configuration.
  * Specifically we want to test whether secret configurations passed through correctly.
  * E.g. 
  * dcos spark run --submit-args="\
        --conf=spark.mesos.driver.secret.names='/path/to/secret' \
        --conf=spark.mesos.driver.secret.envkeys='SECRET_ENV_KEY' \
        --class SecretConfs \
        <jar> isStrictCluster"
  */ 
object SecretConfs {
    implicit val formats = DefaultFormats
    case class AuthResponse(token: String)
    case class SecretResponse(value: String)

    def main(args: Array[String]): Unit = {
        val appName = "SecretConfs"
	var baseUrl = "http://master.mesos"

	if(args.length > 0) {
		if(args(0).toUpperCase().startsWith("T")) {
			baseUrl = "https://master.mesos"
		}
	}
        val authEndPoint = "/acs/api/v1/auth/login"
        val secretEndPoint = "/secrets/v1/secret/default"

        println(s"Running $appName\n")
	
        val conf = new SparkConf().setAppName(appName)
        val secretPath = conf.get("spark.mesos.driver.secret.names", "")
        var secretValue = conf.get("spark.mesos.driver.secret.values", "")
	var secretFile = conf.get("spark.mesos.driver.secret.filenames", "")

        val authToken = postRestContent(baseUrl, authEndPoint)

        if(!secretPath.isEmpty()) {
            secretValue = getRestContent(baseUrl, secretEndPoint, authToken, secretPath, !secretFile.isEmpty())
        }

        println(secretValue)
    }

    def postRestContent(url:String, endPoint:String): String = {
        val httpClient = new DefaultHttpClient
        val httpPost = new HttpPost(url+endPoint)
        val reqContent = """{ "uid": "bootstrapuser", "exp": 0, "password": "deleteme" }"""

        httpPost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        httpPost.setEntity(new StringEntity(reqContent))
        
        val httpResponse = httpClient.execute(httpPost)
        val entity = httpResponse.getEntity
        val resContent = IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)
	println("Fetched Auth Content: " + resContent)
        
        httpClient.getConnectionManager.shutdown

        val resJson = parse(resContent).extract[AuthResponse]
        resJson.token
    }

    def getRestContent(url:String, endPoint:String, authToken:String, secretPath:String, isSecretFile:Boolean): String = {
        val httpClient = new DefaultHttpClient
        val httpGet = new HttpGet(url+endPoint+secretPath)

        httpGet.addHeader(HttpHeaders.AUTHORIZATION, "token="+authToken)

        val httpResponse = httpClient.execute(httpGet)
        val entity = httpResponse.getEntity()
        val resContent = IOUtils.toString(entity.getContent, StandardCharsets.UTF_8)
	println("Fetched Secret Content: " + resContent)

        httpClient.getConnectionManager.shutdown

	if(isSecretFile) {
		resContent
	} else {
	        val resJson = parse(resContent).extract[SecretResponse]
        	resJson.value
	}
    }
}
