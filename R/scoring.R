score_aksjonaer <- function(pre_holders, post_holders) {
  if (is.null(pre_holders) || is.null(post_holders) ||
      nrow(pre_holders) == 0 || nrow(post_holders) == 0) {
    return(list(score = NA_real_, pre_top = NA_character_, post_top = NA_character_))
  }
  pre_clean  <- filter_nominees_dt(pre_holders)
  post_clean <- filter_nominees_dt(post_holders)
  if (nrow(pre_clean) == 0 || nrow(post_clean) == 0) {
    return(list(score = NA_real_, pre_top = NA_character_, post_top = NA_character_))
  }
  pre_agg <- pre_clean[, .(pct = sum(ownership_pct, na.rm = TRUE)), by = holder_id
                       ][order(-pct)][1:min(20, .N)]
  post_agg <- post_clean[, .(pct = sum(ownership_pct, na.rm = TRUE)), by = holder_id
                         ][order(-pct)][1:min(20, .N)]
  joined <- merge(pre_agg, post_agg, by = "holder_id", all = TRUE, suffixes = c(".pre", ".post"))
  joined[is.na(pct.pre), pct.pre := 0]
  joined[is.na(pct.post), pct.post := 0]
  l1 <- sum(abs(joined$pct.pre - joined$pct.post))
  list(
    score = max(0, 1 - l1 / 2),
    pre_top = as.character(jsonlite::toJSON(pre_agg, auto_unbox = TRUE)),
    post_top = as.character(jsonlite::toJSON(post_agg, auto_unbox = TRUE))
  )
}

score_tillitsvalgt <- function(pre_roles, post_roles) {
  if (is.null(pre_roles) || is.null(post_roles) ||
      nrow(pre_roles) == 0 || nrow(post_roles) == 0) {
    return(list(score = NA_real_, pre_roles = NA_character_, post_roles = NA_character_))
  }
  pre <- pre_roles[, .(
    key = paste0(person_id, "|", role_code),
    w = ROLE_WEIGHT[role_code]
  )]
  post <- post_roles[, .(
    key = paste0(person_id, "|", role_code),
    w = ROLE_WEIGHT[role_code]
  )]
  pre[is.na(w), w := 1L]
  post[is.na(w), w := 1L]
  all_keys <- union(pre$key, post$key)
  pre_w <- setNames(pre$w, pre$key)
  post_w <- setNames(post$w, post$key)
  pre_vec <- as.numeric(pre_w[all_keys])
  post_vec <- as.numeric(post_w[all_keys])
  pre_vec[is.na(pre_vec)] <- 0
  post_vec[is.na(post_vec)] <- 0
  mins <- pmin(pre_vec, post_vec)
  maxs <- pmax(pre_vec, post_vec)
  score <- if (sum(maxs) == 0) NA_real_ else sum(mins) / sum(maxs)
  list(
    score = score,
    pre_roles = as.character(jsonlite::toJSON(pre, auto_unbox = TRUE)),
    post_roles = as.character(jsonlite::toJSON(post, auto_unbox = TRUE))
  )
}

score_regnskap <- function(pre_eiendeler, post_eiendeler) {
  if (is.na(pre_eiendeler) || is.na(post_eiendeler) ||
      pre_eiendeler <= 0 || post_eiendeler <= 0) {
    return(list(score = NA_real_, pre = pre_eiendeler, post = post_eiendeler))
  }
  list(
    score = min(1, min(pre_eiendeler, post_eiendeler) / max(pre_eiendeler, post_eiendeler)),
    pre = pre_eiendeler,
    post = post_eiendeler
  )
}
