package sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import jakarta.mail.*;
import jakarta.mail.internet.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class LambdaEnvioEmail implements RequestHandler<Object, String> {

    private final String NOME_BUCKET   = "testeetlcrmetais";
    private final String EMAIL_ORIGEM  = "email.origem@outlook.school";
    private final String EMAIL_SENHA   = "senha";
    private final String EMAIL_DESTINO = "email.destino@outlook.school";

    @Override
    public String handleRequest(Object input, Context context) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String dataHoje    = LocalDateTime.now().format(formatter);
            String nomeArquivo = "Relatório-" + dataHoje + ".json";

            S3Client s3 = S3Client.builder().build();

            ResponseInputStream<GetObjectResponse> s3Object = s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(NOME_BUCKET)
                            .key(nomeArquivo)
                            .build()
            );

            String conteudoJson = new String(s3Object.readAllBytes(), StandardCharsets.UTF_8);

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
            mensagem.setText(conteudoJson);

            Transport.send(mensagem);

            return "E-mail enviado com sucesso para " + EMAIL_DESTINO;

        } catch (Exception e) {
            e.printStackTrace();
            return "Erro na execução: " + e.getMessage();
        }
    }
}