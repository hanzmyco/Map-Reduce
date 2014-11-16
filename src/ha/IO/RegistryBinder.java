package ha.IO;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

public class RegistryBinder implements RegistryBinderInterface {
  Registry localRegistry;
  
  public RegistryBinder(Registry registry) {
    localRegistry = registry;
  }
  
  @Override
  public void bind(String name, Remote thing) throws RemoteException, AlreadyBoundException {
    localRegistry.bind(name, thing);
  }

  @Override
  public Remote lookup(String name) throws RemoteException, NotBoundException {
    return localRegistry.lookup(name);
  }

}
