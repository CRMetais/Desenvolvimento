package sptech.school;

public class Main {

    public static void main(String[] args) {

        LambdaExtracao lambda = new LambdaExtracao();

        LambdaEnvioEmail lambda2 = new LambdaEnvioEmail();

        String resultado = lambda2.handleRequest(null, null);

        System.out.println(resultado);

    }
}