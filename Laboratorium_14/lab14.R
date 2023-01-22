# install.packages("magrittr")
# install.packages("ggplot2")
# install.packages(cluster)

# if(!require(devtools)) install.packages("devtools")
# devtools::install_github("ggobi/ggally")
# install.packages("GGally")
# install.packages("rlang")

library(magrittr)
library(ggplot2)
library("GGally")
library(cluster)

#Zadanie 1.1
lista <- 1:10
print(lista)
#Zadanie 1.2
lista %<>%log2()%>%sin()%>%sum()%>%sqrt()
print(lista)
#Zadanie 1.3
data(iris)
#Zadanie 1.4
print(head(iris))
#Zadanie 1.5
means <- iris %>% aggregate(. ~ Species, ., mean)
print(means)

#Zadanie 2
hist <- ggplot(iris, aes(x = Sepal.Length)) + geom_histogram(aes(fill = Species)) + geom_vline(data = means, aes(xintercept = Sepal.Length, color = Species), linetype = "dashed") + labs(x="Sepal Length", y ="Frequency", title = "Sepal length depending on species")
ggsave("/home/Zadanie2.4.jpg", plot = hist)

#Zadanie 2.6
pairs <- ggpairs(data = iris, aes(color = Species))
ggsave("/home/Zadanie2.6.jpg", plot = pairs)

#Zadanie 3.3
x <- iris[,1:4]
y <- iris[, 5]
#Zadanie 3.4 i 3.5
sum_sqrt = c()
for (i in 1:10){
    kmeans_var <- kmeans(x, i)
    sum_sqrt <- append(sum_sqrt, kmeans_var$tot.withinss)
}

# Zadanie 3.6:
fig36 <- ggplot(data.frame(iteration = 1:length(sum_sqrt), value = sum_sqrt), aes(x = iteration, y = sum_sqrt)) + geom_line()
ggsave("/home/Zadanie3.6.jpg", plot = fig36)

# Zadanie 3.7
kmeans_37 <- kmeans(x, 3)
fig37 <- ggplot(iris, aes(x = Sepal.Length, y = Petal.Length, color = kmeans_37$cluster)) + geom_point()
ggsave("/home/Zadanie3.7.jpg", plot = fig37)

# Zadanie 3.8
fig38 <- ggplot(iris, aes(x = Sepal.Length, y = Petal.Length, color = Species)) + geom_point()
ggsave("/home/Zadanie3.8.jpg", plot = fig38)