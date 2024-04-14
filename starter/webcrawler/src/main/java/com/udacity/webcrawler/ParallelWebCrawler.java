package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> ignoredUrls;
  private final int maxDepth;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(Clock clock, @Timeout Duration timeout,
                     @PopularWordCount int popularWordCount,
                     @TargetParallelism int totalThread,
                     @IgnoredUrls List<Pattern> ignoredUrls,
                     @MaxDepth int maxDepth,
                     PageParserFactory parserFactory){
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(totalThread, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentHashMap<String,Integer> mapCounts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    for(String url: startingUrls) {
      pool.invoke(new InternalParallelWebCrawler(url, deadline, maxDepth, mapCounts, visitedUrls));
    }

    if(mapCounts.isEmpty()) {
      return new CrawlResult.Builder().setWordCounts(mapCounts).setUrlsVisited(visitedUrls.size()).build();
    }

    return new CrawlResult.Builder().setWordCounts(WordCounts.sort(mapCounts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

  public class InternalParallelWebCrawler extends RecursiveTask<Boolean> {
    private String url;
    private Instant deadline;
    private int maxDepth;
    private ConcurrentHashMap<String,Integer> counts;
    private ConcurrentSkipListSet<String> visitedUrls;

    public InternalParallelWebCrawler(String url, Instant deadline, int maxDepth, ConcurrentHashMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
    }


    @Override
    protected Boolean compute() {
      if(maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return false;
      }

      for(Pattern pattern: ignoredUrls) {
        if(pattern.matcher(url).matches()) {
          return false;
        }
      }

      if(visitedUrls.contains(url)) {
        return false;
      }

      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();
      for(ConcurrentHashMap.Entry<String,Integer> map : result.getWordCounts().entrySet()) {
        counts.compute(map.getKey(), (k, v) -> v == null ? map.getValue() : map.getValue() + v);
      }

      List<InternalParallelWebCrawler> subTasks = new ArrayList<>();
      for(String link : result.getLinks()) {
        subTasks.add(new InternalParallelWebCrawler(link, deadline, maxDepth -1, counts, visitedUrls));
      }
      invokeAll(subTasks);
      return true;
    }
  }
}
