## 1) input, if using data on Aster

input <- read.table(file('stdin'), header = FALSE) ## read rows into DF
names(input) <- c('row_id', 'letter', 'y', 'vowel', 'y_positive', 'id_mod_3')

## 2) functions producing a table-like R object

n_rows = nrow(input)
concat = paste(input$letter, collapse = '')
y_sum = sum(input$y)

output = data.frame(n_rows, concat, y_sum)

## 3) write tab-delimited output back to Aster
write.table(output, stdout(), col.names = FALSE, row.names = FALSE, quote = FALSE, sep = '\t')