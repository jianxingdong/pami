function [U, S, V]= HalkoSVD(A, k, l, i)
   n = size(A,2);
   G = normrnd(0,1,n,l);
   HPrev = A*G;
   H = HPrev;
   for t = 1: i
       HPrev = A*(A'*HPrev);
       H = [H HPrev];
   end
   [Q,R] = qr(H,0);
   Q = sparse(Q);
   T = A'*Q;
   [V_,S_,W] = svds(T,(i+1)*l);   
   U_ = Q*W;
   U = sparse(U_(:,1:k));
   V = sparse(V_(:,1:k));
   S = sparse(S_(1:k,1:k));
end