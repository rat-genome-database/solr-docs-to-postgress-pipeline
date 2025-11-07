package edu.mcw.rgd.process;

import edu.mcw.rgd.dao.impl.solr.SolrDocsDAO;
import edu.mcw.rgd.datamodel.solr.SolrDoc;

import java.util.List;

public class SolrDBProcessingThread implements Runnable{
    private final List<SolrDoc> solrDocs;
    private final List<Integer> chunkDataCounts;
    public SolrDBProcessingThread(List<SolrDoc> solrDocs, List<Integer> chunkDataCounts){
        this.solrDocs=solrDocs;
        this.chunkDataCounts=chunkDataCounts;

    }
    @Override
    public void run() {
        SolrDocsDAO solrDocsDAO=new SolrDocsDAO();
        try {
           int chunkedDataCount= solrDocsDAO.addBatch(solrDocs);
           chunkDataCounts.add(chunkedDataCount);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
