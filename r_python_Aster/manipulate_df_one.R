## 1) input, if using data on Aster
input <- read.table(file('stdin'), header = FALSE) ## read rows into DF
names(input) <- c('row_id', 'letter', 'y')


## 2) function producing a table-like R object

output <- transform(input,
                    vowel = grepl('[aeiou]{1}',letter),
                    y_positive = y > 0,
                    id_mod_3 = row_id %% 3
                    )

## 3) write tab-delimited output back to Aster
write.table(output, stdout(), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = '\t')
