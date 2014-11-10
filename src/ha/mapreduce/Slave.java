package ha.mapreduce;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Slave {
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws NumberFormatException, IOException {
    if (args.length != 1) {
      System.out.println("USAGE: java ha.mapreduce.Slave <port>");
      System.exit(0);
    }
    
    @SuppressWarnings("resource")
    ServerSocket newTasksSocket = new ServerSocket(Integer.parseInt(args[0]));
    
    TaskTracker taskTracker = new TaskTracker();

    while (true) {
      System.out.println("[SLAVE] Waiting for new task on port " + args[0] + "...");
      Socket tasksSocket;
      try {
        tasksSocket = newTasksSocket.accept();
        ObjectInputStream newTasksStream = new ObjectInputStream(tasksSocket.getInputStream());
        
        System.out.println("[SLAVE] Getting task information...");
        taskTracker.startTask((Integer) newTasksStream.readObject(), (Class<Task>) newTasksStream.readObject());
        
        Thread.sleep(1000);
        
        newTasksStream.close();
        tasksSocket.close();
      } catch (IOException e1) {
        System.err.println("Cannot accept new task!");
        e1.printStackTrace();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
    }
  }
}
