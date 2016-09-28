## 1) input

## 2) function producing a table-like R object

n_rows <- 26
row_id <- 1:n_rows
letter <- letters[1:n_rows] 
y <- rnorm(n_rows) 

output <- data.frame(row_id, letter, y)


## 3) write tab-delimited output back to Aster
write.table(output, stdout(), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = '\t')
