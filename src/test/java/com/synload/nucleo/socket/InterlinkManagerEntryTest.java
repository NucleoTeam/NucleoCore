package com.synload.nucleo.socket;

import com.synload.nucleo.interlink.InterlinkManager;
import com.synload.nucleo.zookeeper.ServiceInformation;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

public class InterlinkManagerEntryTest {

  @Test
  public void shouldCreateNewEManagerAndVerifyEmpty(){
    InterlinkManager em = new InterlinkManager(null, 2090);



    assertTrue(em.getTopics().size()==0);
  }

  @Test
  public void shouldCreateNewEntryIntoClientList(){

    InterlinkManager em = new InterlinkManager(null, 2090);

    ServiceInformation si = new ServiceInformation();
    si.setHost("test");
    si.setName("test");
    Set<String> events = new HashSet<>();
    events.add("information.test");
    si.setEvents(events);
    em.sync(si);
    assertTrue(em.getConnections().containsKey(si.getName()));
    assertTrue(em.getTopics().size()==1);
  }

  @Test
  public void shouldCreateEventsAndDeleteWhenNodeLeaves(){
    InterlinkManager em = new InterlinkManager(null, 2090);

    ServiceInformation si = new ServiceInformation();
    si.setHost("test");
    si.setName("test");
    Set<String> events = new HashSet<>();
    events.add("information.test");
    si.setEvents(events);
    em.sync(si);
    assertTrue(em.getConnections().containsKey(si.getName()));
    assertTrue(em.getTopics().size()==1);
    em.delete("test");
    assertTrue(em.getTopics().size()==0);
  }
}
