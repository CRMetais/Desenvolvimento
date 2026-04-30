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

    private final String NOME_BUCKET = "testeetlcsvcrmetais";

    @Override
    public String handleRequest(Map<String, String> input, Context context) {

        try {

            String dataInicio = input.get("dataInicio");
            String dataFim = input.get("dataFim");

            HttpClient cliente = HttpClient.newHttpClient();

            String url = "http://localhost:8080/api/etl/full-extract"
                    + "?dataInicio=" + dataInicio
                    + "&dataFim=" + dataFim;


//            Usar essa no deploy
//            String url = System.getenv("BACKEND_URL") + "/api/historico/csv-extract"
//                    + "?dataInicio=" + dataInicio
//                    + "&dataFim=" + dataFim
//                    + "&tipo=" + input.getOrDefault("tipo", "TODOS");

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

            String nomeArquivo = "CSV-" + dataInicio + "-a-" + dataFim + ".csv";

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

            String urlDownload = presigner.presignGetObject(presignRequest)
                    .url()
                    .toString();

            return urlDownload;

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro: " + e.getMessage();
        }
    }

    private String converterParaCSV(String json) {

        StringBuilder csv = new StringBuilder();

        csv.append("id,data,parceiro,produto,pesoKg,precoUnitario,valorTotalItem,tipo,rendimento\n");

        json = json.replace("[", "").replace("]", "");

        String[] linhas = json.split("},\\{");

        for (String linha : linhas) {

            linha = linha.replace("{", "").replace("}", "").replace("\"", "");

            String[] campos = linha.split(",");

            String id = "";
            String data = "";
            String parceiro = "";
            String produto = "";
            String pesoKg = "";
            String preco = "";
            String valor = "";
            String tipo = "";
            String rendimento = "";

            for (String campo : campos) {

                String[] partes = campo.split(":", 2);

                String chave = partes[0];
                String valorCampo = partes.length > 1 ? partes[1] : "";

                switch (chave) {
                    case "id": id = valorCampo; break;
                    case "data": data = valorCampo.split("T")[0]; break;
                    case "parceiro": parceiro = valorCampo; break;
                    case "produto": produto = valorCampo; break;
                    case "pesoKg": pesoKg = valorCampo; break;
                    case "precoUnitario": preco = valorCampo; break;
                    case "valorTotalItem": valor = valorCampo; break;
                    case "tipo": tipo = valorCampo; break;
                    case "rendimento":
                        rendimento = valorCampo.equals("null") ? "" : valorCampo;
                        break;
                }
            }

            csv.append(String.join(",",
                    id,
                    data,
                    parceiro,
                    produto,
                    formatar(pesoKg),
                    formatar(preco),
                    formatar(valor),
                    tipo,
                    formatar(rendimento)
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