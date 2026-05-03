package sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public class LambdaCriarCsv implements RequestHandler<Map<String, String>, String> {

    private final String NOME_BUCKET = System.getenv("BUCKET_NAME");

    @Override
    public String handleRequest(Map<String, String> input, Context context) {

        try {
            String dataInicio = input.get("dataInicio");
            String dataFim = input.get("dataFim");
            String tipo = input.getOrDefault("tipo", "TODOS");

            HttpClient cliente = HttpClient.newHttpClient();

            String backendUrl = System.getenv("BACKEND_URL");
//            String backendUrl = "http://localhost:8080";

            String url = backendUrl + "/historico/csv-extract"
                    + "?dataInicio=" + dataInicio
                    + "&dataFim=" + dataFim
                    + "&tipo=" + tipo;

            HttpRequest requisicao = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = cliente.send(
                    requisicao,
                    HttpResponse.BodyHandlers.ofString()
            );

            String json = response.body();
            String csv = converterParaCSV(json);

            String nomeArquivo = "CSV-" + tipo + "-" + dataInicio + "-a-" + dataFim + ".csv";

            S3Client s3 = S3Client.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(NOME_BUCKET)
                    .key(nomeArquivo)
                    .contentType("text/csv")
                    .build();

            s3.putObject(
                    putObjectRequest,
                    RequestBody.fromString(csv, StandardCharsets.UTF_8)
            );

            S3Presigner presigner = S3Presigner.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(NOME_BUCKET)
                    .key(nomeArquivo)
                    .build();

            GetObjectPresignRequest presignRequest =
                    GetObjectPresignRequest.builder()
                            .signatureDuration(Duration.ofMinutes(10))
                            .getObjectRequest(getObjectRequest)
                            .build();

            return presigner.presignGetObject(presignRequest).url().toString();

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro: " + e.getMessage();
        }
    }

    private String converterParaCSV(String json) {
        StringBuilder csv = new StringBuilder();
        csv.append("id,data,parceiro,produto,pesoKg,precoUnitario,total,rendimento,tipo\n");

        json = json.replace("[", "").replace("]", "");
        String[] linhas = json.split("},\\{");

        for (String linha : linhas) {
            linha = linha.replace("{", "").replace("}", "").replace("\"", "");
            String[] campos = linha.split(",");

            String id = "", data = "", parceiro = "", produto = "",
                    pesoKg = "", preco = "", valor = "", tipo = "", rendimento = "";

            for (String campo : campos) {
                String[] partes = campo.split(":", 2);
                String chave = partes[0].trim();
                String valorCampo = partes.length > 1 ? partes[1].trim() : "";

                switch (chave) {
                    case "id": id = valorCampo; break;
                    case "data": data = valorCampo; break;
                    case "parceiro": parceiro = valorCampo; break;
                    case "produto": produto = valorCampo; break;
                    case "peso": pesoKg = valorCampo; break;
                    case "preco": preco = valorCampo; break;
                    case "total": valor = valorCampo; break;
                    case "tipo": tipo = valorCampo; break;
                    case "rendimento":
                        rendimento = valorCampo.equals("null") ? "" : valorCampo;
                        break;
                }
            }

            csv.append(String.join(",",
                    id, data, parceiro, produto,
                    formatar(pesoKg), formatar(preco), formatar(valor),
                    formatar(rendimento), tipo
            )).append("\n");
        }

        return csv.toString();
    }

    private String formatar(String valor) {
        if (valor == null || valor.isEmpty()) return "";
        try {
            return String.format("%.2f", Double.parseDouble(valor));
        } catch (Exception e) {
            return valor;
        }
    }
}