function e = Error(fea, C)
%fea is the original matrix
%C = fea(:S)

m = size(C,1);
tmp = -sparse(C*sparse(pinv(full(C'*C)))*C');
tmp(1:(m+1):m*m) = diag(tmp) + 1;

e = norm(tmp*fea, 'fro');
end


