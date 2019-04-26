////
////class MyClass {
////    public static void main(String[] args) {
////        Scanner myObj = new Scanner(System.in);
////
////        System.out.println("Enter name, age and salary");
////
////        // String input
////        String name = myObj.nextLine();
////
////        // Numerical input
////        int age = myObj.nextInt();
////        double salary = myObj.nextDouble();
////
////        // Output input by user
////        System.out.println("Name: " + name);
////        System.out.println("Age: " + age);
////        System.out.println("Salary: " + salary);
////    }
////}
//import java.net.*;
//import java.io.*;
//
//public class MyClass {
//    public static void main(String[] args) throws Exception {
//        System.out.println("aa");
//        URL oracle = new URL("http://www.oracle.com/");
//        BufferedReader in = new BufferedReader(
//                new InputStreamReader(oracle.openStream()));
//
//        String inputLine;
//        while ((inputLine = in.readLine()) != null)
//            System.out.println(inputLine);
//        in.close();
//    }
//}


import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MyClass {
    public static void main(String[] args){
        String text = "";
        try{
            String cPath="/ass1_2/twitter4j.properties";
//            URL url = MyClass.class.getResource("twitter4j.properties");
            URL url = MyClass.class.getResource(cPath);
            text = new String(Files.readAllBytes(Paths.get(url.toURI())));
            System.out.println(text);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.out.println(text);
        }

    }
}