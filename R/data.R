library(data.table)
library(arrow)
library(jsonlite)

.cache <- new.env(parent = emptyenv())

load_finstat <- function() {
  if (is.null(.cache$finstat)) {
    fs <- read_parquet(
      gcs_path("finstat/state/snapshots.parquet"),
      col_select = c("organisasjonsnummer", "Regnskapsar",
                     "Regnskapsversjon", "RegnskapstypeKode", "SumEiendeler")
    )
    setDT(fs)
    fs <- fs[Regnskapsversjon == "U" & RegnskapstypeKode == "R"]
    setnames(fs, c("organisasjonsnummer", "Regnskapsar", "SumEiendeler"),
             c("orgnr", "yr", "eiendeler"))
    setkey(fs, orgnr, yr)
    .cache$finstat <- fs[, .(orgnr, yr, eiendeler)]
  }
  .cache$finstat
}

load_roller_current <- function() {
  if (is.null(.cache$roller)) {
    latest <- "roller/parsed/v1/role_persons/2026-04-17.parquet"
    r <- read_parquet(gcs_path(latest))
    setDT(r)
    name_col <- intersect(c("role_code", "roletype_kode", "rolletype_kode"), names(r))[1]
    if (!is.na(name_col) && name_col != "role_code") setnames(r, name_col, "role_code")
    person_col <- intersect(c("person_id", "personid", "identifikator"), names(r))[1]
    if (!is.na(person_col) && person_col != "person_id") setnames(r, person_col, "person_id")
    orgnr_col <- intersect(c("orgnr", "organisasjonsnummer"), names(r))[1]
    if (!is.na(orgnr_col) && orgnr_col != "orgnr") setnames(r, orgnr_col, "orgnr")
    setkey(r, orgnr)
    .cache$roller <- r[, .(orgnr, person_id, role_code)]
  }
  .cache$roller
}

load_aksjeeierbok_year <- function(yr) {
  key <- paste0("akb_", yr)
  if (is.null(.cache[[key]])) {
    path <- gcs_path(paste0("aksjeeierbok/cdc/changelog/", yr, ".parquet"))
    a <- tryCatch(
      read_parquet(path, col_select = c("orgnr", "event_type", "details_json")),
      error = function(e) NULL
    )
    if (is.null(a)) {
      .cache[[key]] <- data.table()
      return(.cache[[key]])
    }
    setDT(a)
    a <- a[event_type != "disappeared"]
    a[, details_str := vapply(details_json, function(x) {
      if (is.raw(x)) rawToChar(x) else as.character(x)
    }, character(1))]
    a[, holder_id := str_match(details_str, '"shareholder_id":"?([^",}]*)"?')[,2]]
    a[, holder_name := str_match(details_str, '"shareholder_name":"?([^",}]*)"?')[,2]]
    a[, ownership_pct := as.numeric(str_match(details_str, '"curr_ownership_pct":([^",}]*)')[,2])]
    a[, details_json := NULL]
    a[, details_str := NULL]
    a[, event_type := NULL]
    setkey(a, orgnr)
    .cache[[key]] <- a
  }
  .cache[[key]]
}

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || is.na(a)) b else a

get_holders <- function(target_orgnr, target_yr) {
  akb <- load_aksjeeierbok_year(target_yr)
  if (nrow(akb) == 0) return(data.table(holder_id = character(), holder_name = character(), ownership_pct = numeric()))
  akb[orgnr == target_orgnr, .(holder_id, holder_name, ownership_pct)]
}

get_roles <- function(target_orgnr) {
  r <- load_roller_current()
  r[orgnr == target_orgnr, .(person_id, role_code)]
}

get_eiendeler <- function(target_orgnr, target_yr) {
  fs <- load_finstat()
  hit <- fs[orgnr == target_orgnr & yr == target_yr]
  if (nrow(hit) == 0) return(NA_real_)
  hit$eiendeler[1]
}

load_edges <- function() {
  e <- read_parquet(gcs_path("entity-lineage/edges.parquet"))
  setDT(e)
  setnames(e, c("source_orgnr", "target_orgnr"),
              c("predecessor_orgnr", "successor_orgnr"))
  e[, event_date := as.Date(event_date)]
  e[, event_yr := as.integer(format(event_date, "%Y"))]
  e
}
