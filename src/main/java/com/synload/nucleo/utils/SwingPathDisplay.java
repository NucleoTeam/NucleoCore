package com.synload.nucleo.utils;

import javax.swing.*;
import com.mxgraph.layout.*;
import com.mxgraph.swing.*;
import com.mxgraph.layout.hierarchical.*;
import com.mxgraph.layout.orthogonal.*;
import com.synload.nucleo.chain.path.PathBuilder;
import com.synload.nucleo.chain.path.PathGenerationException;
import com.synload.nucleo.examples.BroadcastService;
import org.jgrapht.Graph;
import org.jgrapht.ext.JGraphXAdapter;

import java.awt.*;

public class SwingPathDisplay extends
    JApplet {
    Graph graph;

    public static void display(String title, Graph graph){
        SwingPathDisplay applet = new SwingPathDisplay();
        applet.setGraph(graph);
        applet.init();

        JFrame frame = new JFrame();
        frame.getContentPane().add(applet);
        frame.setTitle(title);
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
    @Override
    public void init() {
        Dimension DEFAULT_SIZE = new Dimension(1600, 900);
        JGraphXAdapter jgxAdapter = new JGraphXAdapter<>(graph);
        mxGraphComponent component = new mxGraphComponent(jgxAdapter);
        component.setConnectable(false);
        component.getGraph().setAllowDanglingEdges(false);
        getContentPane().add(component);
        resize(DEFAULT_SIZE);
        mxHierarchicalLayout layout = new mxHierarchicalLayout(jgxAdapter);

        layout.execute(jgxAdapter.getDefaultParent());
    }

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }
}
