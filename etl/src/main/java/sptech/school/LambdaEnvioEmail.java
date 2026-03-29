package sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import jakarta.mail.*;
import jakarta.mail.internet.*;

import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Properties;

public class LambdaEnvioEmail implements RequestHandler<Object, String> {

    private final String NOME_BUCKET   = "testeetlcrmetais";
    private final String EMAIL_ORIGEM  = "email_origem@teste.com";
    private final String EMAIL_SENHA   = "$4/=4D!nh0";
    private final String EMAIL_DESTINO = "email_destino@teste.com";

    @Override
    public String handleRequest(Object input, Context context) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String dataHoje    = LocalDateTime.now().format(formatter);
            String nomeArquivo = "Relatório-" + dataHoje + ".json";

            // 1. Buscar arquivo JSON do S3
            S3Client s3 = S3Client.builder().build();
            ResponseInputStream<GetObjectResponse> s3Object = s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(NOME_BUCKET)
                            .key(nomeArquivo)
                            .build()
            );
            String conteudoJson = new String(s3Object.readAllBytes(), StandardCharsets.UTF_8);

            // 2. Processar o JSON e gerar resumo
            String resumo = processarJson(conteudoJson);

            // 3. Configurar e enviar e-mail
            Properties props = new Properties();
            props.put("mail.smtp.auth",            "true");
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.host",            "smtp.office365.com");
            props.put("mail.smtp.port",            "587");

            Session session = Session.getInstance(props, new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(EMAIL_ORIGEM, EMAIL_SENHA);
                }
            });

            Message mensagem = new MimeMessage(session);
            mensagem.setFrom(new InternetAddress(EMAIL_ORIGEM));
            mensagem.setRecipients(Message.RecipientType.TO, InternetAddress.parse(EMAIL_DESTINO));
            mensagem.setSubject("Relatório Diário ETL - " + dataHoje);
            mensagem.setContent(resumo, "text/html; charset=utf-8");

            Transport.send(mensagem);

            return "E-mail enviado com sucesso para " + EMAIL_DESTINO;

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro na execução: " + e.getMessage();
        }
    }

    private String processarJson(String conteudoJson) {
        JSONArray itens = new JSONArray(conteudoJson);


        List<JSONObject> compras = new ArrayList<>();
        List<JSONObject> vendas  = new ArrayList<>();

        for (int i = 0; i < itens.length(); i++) {
            JSONObject item = itens.getJSONObject(i);
            if ("COMPRA".equals(item.getString("tipo"))) {
                compras.add(item);
            } else {
                vendas.add(item);
            }
        }


        Set<String> parceirosEntrega  = new HashSet<>();
        Set<String> parceirosRetirada = new HashSet<>();

        double qtdMaterialEntrega  = 0;
        double qtdMaterialRetirada = 0;
        double pesoLataSolta       = 0;
        double pagamentoNotas      = 0;

        for (JSONObject item : compras) {
            parceirosEntrega.add(item.getString("parceiro"));
            qtdMaterialEntrega += item.getDouble("pesoKg");
            pagamentoNotas     += item.getDouble("valorTotalItem");
            if ("Lata".equals(item.getString("produto"))) {
                pesoLataSolta += item.getDouble("pesoKg");
            }
        }

        for (JSONObject item : vendas) {
            parceirosRetirada.add(item.getString("parceiro"));
            qtdMaterialRetirada += item.getDouble("pesoKg");
        }

        int clientesEntrega  = parceirosEntrega.size();
        int clientesRetirada = parceirosRetirada.size();
        int totalClientes    = clientesEntrega + clientesRetirada;
        double pesoTotal     = qtdMaterialEntrega + qtdMaterialRetirada;


        return String.format("""
                <html>
                <body style="font-family: Arial, sans-serif; padding: 20px;">
                    <h2 style="color: #333;">Relatório Diário - CR Metais</h2>
                    <table border="1" cellpadding="8" cellspacing="0"
                           style="border-collapse: collapse; width: 100%%;">
                        <tr style="background-color: #f2f2f2;">
                            <th>Campo</th>
                            <th>Valor</th>
                        </tr>
                        <tr>
                            <td>Clientes atendidos (entrega)</td>
                            <td style="background-color: #ffff00; font-weight: bold;">%d</td>
                        </tr>
                        <tr>
                            <td>Clientes atendidos (retirada)</td>
                            <td style="background-color: #ffff00; font-weight: bold;">%d</td>
                        </tr>
                        <tr>
                            <td><b>Total de clientes</b></td>
                            <td><b>%d</b></td>
                        </tr>
                        <tr>
                            <td>Qtd de material (entrega)</td>
                            <td>%.2f</td>
                        </tr>
                        <tr>
                            <td>Qtd de material (retirada)</td>
                            <td>%.2f</td>
                        </tr>
                        <tr>
                            <td><b>Peso total (Kg)</b></td>
                            <td><b>%.2f</b></td>
                        </tr>
                        <tr>
                            <td>Peso de lata solta</td>
                            <td>%.2f</td>
                        </tr>
                        <tr>
                            <td><b>Pagamento de Notas</b></td>
                            <td><b>R$ %.2f</b></td>
                        </tr>
                    </table>
                </body>
                </html>
                """,
                clientesEntrega,
                clientesRetirada,
                totalClientes,
                qtdMaterialEntrega,
                qtdMaterialRetirada,
                pesoTotal,
                pesoLataSolta,
                pagamentoNotas
        );
    }
}