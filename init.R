my_packages = c("bigrquery", "rvest", 'stringr','shinyFiles','xml2')

install_if_missing = function(p) {
  if (p %in% rownames(installed.packages()) == FALSE) {
    install.packages(p)
  }
}

invisible(sapply(my_packages, install_if_missing))