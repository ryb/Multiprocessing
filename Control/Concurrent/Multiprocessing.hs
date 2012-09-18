module Control.Concurrent.Multiprocessing
    (parMap, parallel, parallel_, parForM, parForM_, splitIntoBitsOf)
    where

import Prelude hiding (catch)
import Control.Concurrent.MVar
import Control.Concurrent.ParallelIO.Local hiding (parallel_)
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Data.Either (partitionEithers)
import GHC.Conc
import System.IO
import qualified Control.Monad.Par as Par

-- |
--
-- >>> splitIntoBitsOf 8 [1..20]
-- [[1,2,3,4,5,6,7,8],[9,10,11,12,13,14,15,16],[17,18,19,20]]
splitIntoBitsOf :: Int -> [a] -> [[a]]
splitIntoBitsOf _ [] = [[]]
splitIntoBitsOf n xs    | not $ null ys' = ys : (splitIntoBitsOf n ys')
                        | otherwise = [ys]
    where
        ys = take n xs
        ys' = drop n xs

-- edited from Control/Concurrent/ParallelIO/Local.hs because original has space leak (defined as @parallel pool xs >> return ()@)
-- | 'parallel' with its result discarded.
parallel_ :: Pool -> [IO a] -> IO ()
parallel_ pool acts = mask $ \restore -> do
        main_tid <- myThreadId
        resultvars <- forM acts $ \act -> do
            resultvar <- newEmptyMVar
            _tid <- forkIO $ bracket_ (killPoolWorkerFor pool) (spawnPoolWorkerFor pool) $
                reflectExceptionsTo main_tid $ do
                    res <- restore act
                    True <- tryPutMVar resultvar res
                    return ()
            return resultvar
        extraWorkerWhileBlocked pool (mapM_ takeMVar resultvars)
    where
        reflectExceptionsTo :: ThreadId -> IO () -> IO ()
        reflectExceptionsTo tid act = act `catch` \e -> throwTo tid (e :: SomeException)

-- | 'parallel' with its arguments flipped.
parForM xs action = withPool numCapabilities $ \pool -> do
    let splitted = splitIntoBitsOf numCapabilities xs
    (errs, results) <- fmap (partitionEithers . concat) $
        (forM (map (map action) splitted) $ \s -> do
            rs <- parallelInterleavedE pool s
            rs `pseq` return rs)
    return $ (errs, results)

-- | 'parallel_' with its arguments flipped.
parForM_ xs action = withPool numCapabilities $ \pool -> do
    let splitted = splitIntoBitsOf numCapabilities xs
    (errs, _) <- fmap (partitionEithers . concat) $
        (forM (map (map action) splitted) $ \s -> do
            rs <- parallelInterleavedE pool s
            rs `pseq` return rs)
    when (not $ null errs) $ hPutStrLn stderr ("parForM_ exceptions: " ++
        show errs)

-- | Apply a given function to each argument in the list in parallel with
-- execution batched to the number of capabilities (cores) of the machine
-- at a time.
parMap :: (NFData b) => (a -> b) -> [a] -> [b]
parMap f xs = concat $ map (Par.runPar . Par.parMap f) $
    splitIntoBitsOf numCapabilities xs
