import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { Outlet } from "react-router"
import { Container } from "./components/partials/Container"
import { Header } from "./components/partials/Header"
import { Footer } from "./components/partials/Footer"


function App() {

const queryClient = new QueryClient()

  return (
    <QueryClientProvider client={queryClient}>
      <Header />
      <Container >
        <Outlet/>
      </Container>
      <Footer />
    </QueryClientProvider>
  )
}

export default App
