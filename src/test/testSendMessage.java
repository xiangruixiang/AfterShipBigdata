package MongoDB;

import GoogleService.GooglePubSub.PublishMessage;

import java.util.ArrayList;
import java.util.List;

public class testSendMessage {

    public static void main( String args[] ){

         final String GCPprojectId = "aftership-team-data";
         final String GCPtopic = "mysub";


        List<String> dataList = new ArrayList<>();

        dataList.add("1111111");
        dataList.add("22222");

        try {
            PublishMessage.publishMessagesWithErrorHandler(dataList, GCPprojectId, GCPtopic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
