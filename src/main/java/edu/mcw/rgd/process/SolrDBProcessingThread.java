package edu.mcw.rgd.process;

import edu.mcw.rgd.dao.impl.solr.SolrDocsDAO;
import edu.mcw.rgd.datamodel.solr.SolrDoc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
//            Set<SolrDoc> limitedDocs=new HashSet<>();
//            int limitedDocCount=2;
//            for(SolrDoc doc:solrDocs){
//                if(limitedDocCount>0){
//                    limitedDocs.add(doc);
//                }else break;
//                limitedDocCount--;
//            }
//            System.out.println("Limited SolrDoc Count:"+ limitedDocs.size());
//            for(SolrDoc doc:limitedDocs){
//                System.out.println("PMID:"+ doc.getPmid());
//            }
//            int chunkedDataCount= solrDocsDAO.addBatch(new ArrayList<>(limitedDocs));
//          chunkDataCounts.add(chunkedDataCount);
           int chunkedDataCount= solrDocsDAO.addBatch(solrDocs);
           chunkDataCounts.add(chunkedDataCount);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
