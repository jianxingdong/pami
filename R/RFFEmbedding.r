library(MASS)
#Embedding based on Random Fourier Features
#X original data matrix, an instance per column
#r number of fourier features
# a function that returns r fourier components for the desired kernel function. The function returns an rxm matrix where m is the dimension of the original data
#fcParams params to be passed to the function FourierComponents
rffEmbedding <- function(X, r, FourierComponents, fcParams){
  fComp <- FourierComponents(dim(X)[1],r,fcParams)
  n <- dim(X)[2]  
  tmpFtrs <- fComp%*%X 
  Y <- rbind(cos(tmpFtrs), sin(tmpFtrs))
  return(Y)  
}

#r is the number of the desired fourier components
#m the dimension of the components to be generated
#return rxm matrix
RBFFourierComponents <- function(m,r,fcParams){ 
 return(mvrnorm(r,rep(0,m),diag(1/fcParams$sigma,nrow=m,ncol=m)))
}

#fcParams <- list()
#fcParams$sigma <- 0.2
#Y <- rffEmbedding(t(test$x),20,RBFFourierComponents, fcParams)
#approxKernel <- crossprod(Y[,1:100])
#kernel <- rbfdot(sigma=0.2)
#fk <- FullKernel(t(test$x)[,1:100],kernel)
#norm(approxKernel-fk, type="F")