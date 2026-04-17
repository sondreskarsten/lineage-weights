library(data.table)
library(arrow)
library(dplyr)
library(stringr)

.cache <- new.env(parent = emptyenv())

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || is.na(a)) b else a

load_finstat <- function() {
  if (is.null(.cache$finstat)) {
    fs <- open_dataset(gcs_path("finstat/state/snapshots.parquet")) |>
      filter(Regnskapsversjon == "U", RegnskapstypeKode == "R") |>
      select(orgnr = organisasjonsnummer,
             yr = Regnskapsar,
             eiendeler = SumEiendeler) |>
      collect() |>
      as.data.table()
    setkey(fs, orgnr, yr)
    .cache$finstat <- fs
  }
  .cache$finstat
}

load_roller_current <- function() {
  if (is.null(.cache$roller)) {
    path <- gcs_path("roller/parsed/v1/role_persons/2026-04-17.parquet")
    r <- read_parquet(path) |> as.data.table()
    if (!"role_code" %in% names(r) && "roletype_kode" %in% names(r)) {
      setnames(r, "roletype_kode", "role_code")
    }
    if (!"role_code" %in% names(r) && "rolletype_kode" %in% names(r)) {
      setnames(r, "rolletype_kode", "role_code")
    }
    if (!"person_id" %in% names(r) && "personid" %in% names(r)) {
      setnames(r, "personid", "person_id")
    }
    if (!"person_id" %in% names(r) && "identifikator" %in% names(r)) {
      setnames(r, "identifikator", "person_id")
    }
    if (!"orgnr" %in% names(r) && "organisasjonsnummer" %in% names(r)) {
      setnames(r, "organisasjonsnummer", "orgnr")
    }
    cols <- intersect(c("orgnr", "person_id", "role_code"), names(r))
    r <- r[, ..cols]
    setkey(r, orgnr)
    .cache$roller <- r
  }
  .cache$roller
}

load_aksjeeierbok_year <- function(yr) {
  key <- paste0("akb_", yr)
  if (is.null(.cache[[key]])) {
    path <- gcs_path(paste0("aksjeeierbok/cdc/changelog/", yr, ".parquet"))
    a <- tryCatch(
      open_dataset(path) |>
        filter(event_type != "disappeared") |>
        select(orgnr, details_json) |>
        collect() |>
        as.data.table(),
      error = function(e) NULL
    )
    if (is.null(a) || nrow(a) == 0) {
      .cache[[key]] <- data.table()
      return(.cache[[key]])
    }
    a[, details_str := vapply(details_json, function(x) {
      if (is.raw(x)) rawToChar(x) else as.character(x)
    }, character(1))]
    a[, holder_id := str_match(details_str, '"shareholder_id":"?([^",}]*)"?')[, 2]]
    a[, holder_name := str_match(details_str, '"shareholder_name":"?([^",}]*)"?')[, 2]]
    a[, ownership_pct := as.numeric(str_match(details_str, '"curr_ownership_pct":([^",}]*)')[, 2])]
    a[, c("details_json", "details_str") := NULL]
    setkey(a, orgnr)
    .cache[[key]] <- a
  }
  .cache[[key]]
}

get_holders <- function(target_orgnr, target_yr) {
  akb <- load_aksjeeierbok_year(target_yr)
  if (nrow(akb) == 0) {
    return(data.table(holder_id = character(), holder_name = character(), ownership_pct = numeric()))
  }
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
