package sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.nio.charset.StandardCharsets;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import software.amazon.awssdk.core.sync.RequestBody;

public class LambdaExtracao implements RequestHandler<Object, String> {

    private final String NOME_BUCKET = "testeetlcrmetais";

    @Override
    public String handleRequest(Object input, Context context) {

        try {

            // Criar cliente HTTP
            HttpClient cliente = HttpClient.newHttpClient();

            // Chamar endpoint ETL
            HttpRequest requisicao = HttpRequest.newBuilder()
                    .uri(URI.create("http://10.18.34.173:8080/api/etl/full-extract"))
                    .GET()
                    .build();

            HttpResponse<String> response = cliente.send(
                    requisicao,
                    HttpResponse.BodyHandlers.ofString()
            );

            String body = response.body();

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String dataHoje = LocalDateTime.now().format(formatter);

            String nomeArquivo = "Relatório-" + dataHoje + ".json";

            S3Client s3 = S3Client.builder().build();

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(NOME_BUCKET)
                    .key(nomeArquivo)
                    .build();

            // Enviar arquivo para o S3
            s3.putObject(
                    putObjectRequest,
                    RequestBody.fromString(body, StandardCharsets.UTF_8)
            );

            return "Arquivo enviado com sucesso: " + nomeArquivo;

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro na execução: " + e.getMessage();
        }
    }
}