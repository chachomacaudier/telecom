package teco.eventMessage;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
//import java.util.Base64;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.eclipse.jetty.util.security.Password;

/**
 * Clase que agrupa comportamiento util para el manejo datos en la transformacion entre sus diversos formatos
 * 
 * @author u190438
 *
 */
public class Utils {
	
	// Reemplaza caracteres especiales para evitar problemas en las sentencias.
	static public String procesarCaracteresEspeciales(String dato) {
		if (dato == null)
			return "";

		if (!("".equals(dato))){
			dato = dato.replaceAll("\"", "&quot;");
			dato = dato.replaceAll("'" , "&apos;");
			dato = dato.replaceAll("&", "&amp;");
			dato = dato.replaceAll("<" , "&lt;");
			dato = dato.replaceAll(">" , "&gt;");
		}
		return dato;
	}
	
	/**
	 * Retorna un String con el contenido de inputStream.
	 * 
	 * @param inputStream
	 * @return
	 */
	public static String toString(InputStream inputStream) {
	    BufferedReader reader = new BufferedReader(
	        new InputStreamReader(inputStream));
	    return reader.lines().collect(Collectors.joining(
	        System.getProperty("line.separator")));
	}
	
	/**
	 * Encripta el password pasado y retorna el string resultante.
	 * 
	 * @param pass, un String con el password a encriptar
	 * @return, un string con el password encriptado.
	 */
	public static String encrypt(String pass) {
		return Password.obfuscate(pass);
		//return Base64.getEncoder().encodeToString(pass.getBytes());
	}

	/**
	 * Desencripta el string pasado (un password) y lo retorna.
	 * 
	 * @param pass, string encriptado.
	 * @return, el string desencriptado
	 */
	public static String decrypt(String pass) {
		return Password.deobfuscate(pass);
		//return new String(Base64.getDecoder().decode(pass));
	}
	
	/**
	 * Esto deshabilitará completamente la verificación de SSL
	 */
	public static void installAllTrustingTrusManager() {
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[]{
		    new X509TrustManager() {
		        public X509Certificate[] getAcceptedIssuers() {
		            return null;
		        }
		        public void checkClientTrusted(
		            X509Certificate[] certs, String authType) {
		        }
		        public void checkServerTrusted(
		            X509Certificate[] certs, String authType) {
		        }
		    }
		};

		// Install the all-trusting trust manager
		try {
		    SSLContext sc = SSLContext.getInstance("SSL");
		    sc.init(null, trustAllCerts, new SecureRandom());
		    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (Exception e) {
		}
	}
}
