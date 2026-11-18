package main

import (
	"bufio"
	"cmp"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path"
	"slices"
	"strconv"
	s "strings"
	"sync"
	"time"
)

const numWorkers = 5

var jobsCh = make(chan string, numWorkers*1000)
var resultsCh = make(chan map[string]*campaignStats, numWorkers)

type campaign struct {
	campaign_id string
	date        string
	impressions int64
	clicks      int64
	spend       float64
	conversions int64
}

type campaignStats struct {
	campaign_id       string
	total_impressions int64
	total_clicks      int64
	total_spend       float64
	total_conversions int64
}

type campaignResult struct {
	campaignStats
	CTR float64
	CPA float64
}

func readFile(inputPath string) {
	defer close(jobsCh)

	f, err := os.Open(inputPath)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return
		}
		return
	}

	for scanner.Scan() {
		line := scanner.Text()
		jobsCh <- line
		// fmt.Println(line)
	}

	if err := scanner.Err(); err != nil {
		return
	}
}

func parseObject(line string) (campaign, bool) {
	parts := s.Split(line, ",")
	campaign_id := parts[0]
	date := parts[1]

	impressions, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return campaign{}, false
	}

	clicks, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return campaign{}, false
	}

	spend, err := strconv.ParseFloat(parts[4], 64)
	if err != nil {
		return campaign{}, false
	}

	conversions, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return campaign{}, false
	}

	return campaign{
		campaign_id: campaign_id,
		date:        date,
		impressions: impressions,
		clicks:      clicks,
		spend:       spend,
		conversions: conversions,
	}, true
}

func handle() {
	statsMap := make(map[string]*campaignStats)

	for line := range jobsCh {
		campaign, ok := parseObject(line)
		if !ok {
			return
		}

		// fmt.Println(campaign)
		stats, exists := statsMap[campaign.campaign_id]
		if !exists || stats == nil {
			stats = &campaignStats{
				campaign_id: campaign.campaign_id,
			}
		}
		stats.total_impressions += campaign.impressions
		stats.total_clicks += campaign.clicks
		stats.total_spend += campaign.spend
		stats.total_conversions += campaign.conversions
		// fmt.Println(stats)

		statsMap[campaign.campaign_id] = stats
	}

	resultsCh <- statsMap
}

func getTop10CampaignsWithHighestCTR(dataset []campaignResult) []campaignResult {
	copied := slices.Clone(dataset)
	slices.SortFunc(copied, func(a campaignResult, b campaignResult) int { return cmp.Compare(b.CTR, a.CTR) })
	limit := min(10, len(dataset))
	return copied[:limit]
}

func getTop10CampaignsWithLowestCPA(dataset []campaignResult) []campaignResult {

	var datasetWithoutZeroConversion []campaignResult

	for _, c := range dataset {
		if c.total_conversions > 0 {
			datasetWithoutZeroConversion = append(datasetWithoutZeroConversion, c)
		}
	}

	slices.SortFunc(datasetWithoutZeroConversion, func(a campaignResult, b campaignResult) int { return cmp.Compare(a.CPA, b.CPA) })
	limit := min(10, len(datasetWithoutZeroConversion))
	return datasetWithoutZeroConversion[:limit]
}

func writeFile(outputPath string, rows []campaignResult) error {
	err := os.MkdirAll(path.Dir(outputPath), 0755)
	if err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"campaign_id", "total_impressions", "total_clicks",
		"total_spend", "total_conversions", "CTR", "CPA",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	for _, row := range rows {
		data := []string{
			row.campaign_id,
			strconv.FormatInt(row.total_impressions, 10),
			strconv.FormatInt(row.total_clicks, 10),
			strconv.FormatFloat(row.total_spend, 'f', 2, 64),
			strconv.FormatInt(row.total_conversions, 10),
			strconv.FormatFloat(row.CTR, 'f', 3, 64),
			strconv.FormatFloat(row.CPA, 'f', 3, 64),
		}

		if err := writer.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	startTime := time.Now()

	inputPath := flag.String("input", "...", "...")
	outputDir := flag.String("output", "...", "...")
	flag.Parse()

	var wg sync.WaitGroup

	file_path := path.Join(*inputPath)
	wg.Go(func() { readFile(file_path) })

	for range numWorkers {
		wg.Go(func() { handle() })
	}

	wg.Wait()
	close(resultsCh)

	statsMap := make(map[string]*campaignStats)
	for localMap := range resultsCh {
		for k, v := range localMap {
			stats, exists := statsMap[k]
			if !exists || stats == nil {
				statsMap[k] = v
			} else {
				stats.total_impressions += v.total_impressions
				stats.total_clicks += v.total_clicks
				stats.total_spend += v.total_spend
				stats.total_conversions += v.total_conversions
				statsMap[k] = stats
			}
		}
	}

	var results []campaignResult
	for k, v := range statsMap {
		CTR := 0.0
		if v.total_impressions > 0 {
			CTR = float64(v.total_clicks) / float64(v.total_impressions)
		}
		CPA := 0.0
		if v.total_conversions > 0 {
			CPA = v.total_spend / float64(v.total_conversions)
		}

		results = append(results, campaignResult{
			campaignStats: *statsMap[k],
			CTR:           CTR,
			CPA:           CPA,
		})
	}

	top10CTR := getTop10CampaignsWithHighestCTR(results)
	top10CPA := getTop10CampaignsWithLowestCPA(results)
	// fmt.Println(top10CTR)
	// fmt.Println(top10CPA)

	writeFile(path.Join(*outputDir, "top10_ctr.csv"), top10CTR)
	writeFile(path.Join(*outputDir, "top10_cpa.csv"), top10CPA)

	fmt.Println(time.Since(startTime))
}
