library(data.table)
library(arrow)
library(jsonlite)
library(stringr)
library(logger)

options(error = function() {
  traceback(2)
  if (!interactive()) quit(status = 1, save = "no")
})

source("R/utils.R")
source("R/scoring.R")
source("R/data.R")

main <- function() {
  log_info("lineage-weights v{METHODOLOGY_VERSION} starting")
  t0 <- Sys.time()

  log_info("loading edges")
  edges <- load_edges()
  log_info("loaded {nrow(edges)} edges")

  log_info("pre-loading finstat + roller")
  load_finstat()
  load_roller_current()
  log_info("warm: finstat {nrow(.cache$finstat)} rows, roller {nrow(.cache$roller)} rows")

  setorder(edges, event_yr)
  unique_yrs <- sort(unique(edges$event_yr))
  log_info("edges span {min(unique_yrs)}..{max(unique_yrs)}")

  dir.create("/tmp/year_batches", showWarnings = FALSE)

  for (yr in unique_yrs) {
    batch <- edges[event_yr == yr]
    log_info("year {yr}: {nrow(batch)} edges; loading aksjeeierbok...")
    load_aksjeeierbok_year(yr - 1)
    load_aksjeeierbok_year(yr)
    log_info("  pre-yr akb: {nrow(.cache[[paste0('akb_', yr-1)]])} rows; post-yr: {nrow(.cache[[paste0('akb_', yr)]])} rows")

    yr_output <- vector("list", nrow(batch))
    for (i in seq_len(nrow(batch))) {
      e <- batch[i]
      pre_holders  <- get_holders(e$predecessor_orgnr, yr - 1)
      post_holders <- get_holders(e$successor_orgnr, yr)
      pre_roles  <- get_roles(e$predecessor_orgnr)
      post_roles <- get_roles(e$successor_orgnr)
      pre_eiendeler  <- get_eiendeler(e$predecessor_orgnr, yr - 1)
      post_eiendeler <- get_eiendeler(e$successor_orgnr, yr)

      sa <- score_aksjonaer(pre_holders, post_holders)
      st <- score_tillitsvalgt(pre_roles, post_roles)
      sr <- score_regnskap(pre_eiendeler, post_eiendeler)

      yr_output[[i]] <- data.table(
        predecessor_orgnr = as.character(e$predecessor_orgnr),
        successor_orgnr   = as.character(e$successor_orgnr),
        event_date        = e$event_date,
        event_type        = as.character(e$event_type),
        kid               = as.character(e$kid),
        confidence        = as.numeric(e$confidence),
        aksjonaer_score   = as.numeric(sa$score),
        tillitsvalgt_score = as.numeric(st$score),
        regnskap_score    = as.numeric(sr$score),
        aksjonaer_pre_top = as.character(sa$pre_top),
        aksjonaer_post_top = as.character(sa$post_top),
        tillitsvalgt_pre_roles  = as.character(st$pre_roles),
        tillitsvalgt_post_roles = as.character(st$post_roles),
        regnskap_pre_eiendeler  = as.numeric(sr$pre),
        regnskap_post_eiendeler = as.numeric(sr$post),
        methodology_version = METHODOLOGY_VERSION,
        computed_at         = Sys.time()
      )
    }
    yr_dt <- rbindlist(yr_output, use.names = TRUE, fill = TRUE)
    write_parquet(yr_dt, paste0("/tmp/year_batches/", yr, ".parquet"))
    log_info("  wrote year {yr}: {nrow(yr_dt)} rows")

    .cache[[paste0("akb_", yr - 1)]] <- NULL
    if (yr > min(unique_yrs)) .cache[[paste0("akb_", yr - 2)]] <- NULL
    gc(verbose = FALSE)

    if (yr %% 5 == 0) {
      log_info("  elapsed {round(difftime(Sys.time(), t0, units='mins'), 1)}min")
    }
  }

  log_info("reading back year batches")
  year_files <- list.files("/tmp/year_batches", full.names = TRUE, pattern = "\\.parquet$")
  batches <- lapply(year_files, read_parquet)
  result <- rbindlist(batches, use.names = TRUE, fill = TRUE)
  log_info("result: {nrow(result)} rows, {ncol(result)} cols")

  out_path <- gcs_path("entity-lineage/weighted_edges.parquet")
  log_info("writing to {out_path}")
  write_parquet(result, out_path, compression = "zstd")
  log_info("done in {round(difftime(Sys.time(), t0, units='mins'), 1)} min")
}

main()
