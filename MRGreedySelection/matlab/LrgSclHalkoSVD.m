function [ U,S,V, E ] = LrgSclHalkoSVD(matrixDir,k,l,i,numRows )
%column-based partitioning is assumed
%the algorithm is applied to A' then, U and V are reversed
%numRows: number of rows of the original matrix A
%E is the approximation Error

   G = normrnd(0,1,numRows,l);   
   H0 = [];
   partsList = dir(matrixDir);
   numParts = size(partsList,1); %will have to skip . and ..   
   
   for itr = 3: numParts
    load(strcat(matrixDir,partsList(itr).name),'A');
    A = A';
    H0 = [H0; A*G];
   end   
   
   H = H0;   
   Hprev = H0;
   for t = 1: i
       F = zeros(numRows,l);       
       from = 1;
       to = 0;
       for itr = 3: numParts
           load(strcat(matrixDir,partsList(itr).name),'A');
           r = size(A,2);
           to = to + r;
           F = F + A*Hprev(from:to,:);
           from = from + r;
       end
       
       Ht = [];
       for itr = 3: numParts
            load(strcat(matrixDir,partsList(itr).name),'A');
            A = A';
            Ht = [Ht; A*F];
       end
       
       Hprev = Ht;
       H = [H Ht];
   end
   [Q,R] = qr(H, 0);
   T = zeros(numRows,(i+1)*l);
   from = 1;
   to = 0;
   for itr = 3: numParts
    load(strcat(matrixDir,partsList(itr).name),'A');
    r = size(A,2);
    to = to + r;
    T = T + A*Q(from:to,:);
    from = from + r;
   end   
   
   [V_,S_,W] = svd(T);
   U_ = Q*W;
   V = U_(:,1:k);
   U = V_(:,1:k);
   S = S_(1:k,1:k);

% AF: Please note that this method has two parameters l (the number of columns of G) and k (the rank of the resulting matrix). For our current implemtation of the method, l=k. We can extend our implementation to calulcate rank k approximmation where k<1. We can do this if we have time.
   
%Col Space of U is the projection Space
C = U;
%calc E
E = 0;
for itr = 3: numParts
    load(strcat(matrixDir,partsList(itr).name),'A');
    E = E + Error(A,C);
end

end
