package edu.mcw.rgd.process;

import edu.mcw.rgd.dao.impl.solr.SolrDocsDAO;
import edu.mcw.rgd.datamodel.solr.SolrDoc;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread for updating existing PMID records in the database
 */
public class SolrDBUpdateThread implements Runnable {
    private final List<SolrDoc> solrDocs;
    private final List<Integer> chunkDataCounts;

    public SolrDBUpdateThread(List<SolrDoc> solrDocs, List<Integer> chunkDataCounts) {
        this.solrDocs = solrDocs;
        this.chunkDataCounts = chunkDataCounts;
    }

    @Override
    public void run() {
        SolrDocsDAO solrDocsDAO = new SolrDocsDAO();
        try {
            System.out.println("Updating existing SolrDocs count: " + solrDocs.size());
            for (SolrDoc doc : solrDocs) {
                System.out.println("Updating PMID: " + doc.getPmid());
            }
            int updatedCount = solrDocsDAO.updateBatch(solrDocs);
            chunkDataCounts.add(updatedCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
