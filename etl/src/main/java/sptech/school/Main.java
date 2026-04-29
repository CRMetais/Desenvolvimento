package sptech.school;

import java.util.Map;
import java.awt.Desktop;
import java.net.URI;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) {

        LambdaExtracao lambda = new LambdaExtracao();

        LambdaEnvioEmail lambda2 = new LambdaEnvioEmail();

        LambdaCriarCsv lambda3 = new LambdaCriarCsv();

        Map<String, String> input = Map.of(
                "dataInicio", "2026-04-01",
                "dataFim", "2026-04-15"
        );

        String resultado = lambda3.handleRequest(input, null);

        System.out.println("URL gerada:");
        System.out.println(resultado);

        try {
            if (Desktop.isDesktopSupported()) {
                Desktop.getDesktop().browse(new URI(resultado));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}