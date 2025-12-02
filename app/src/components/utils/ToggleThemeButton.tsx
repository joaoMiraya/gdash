
import { useTheme } from "@/hooks/useTheme"
import { Button } from "../ui/button"
import { Moon, SunDim } from "lucide-react";


export const ToggleThemeButton = () => {
    const theme = useTheme();

    return (
  
        <Button 
            onClick={() => theme.toggleTheme()}
            variant="outline"
            size="icon"
            aria-label="Toggle Theme"
            className="fixed top-4 right-4 cursor-pointer"
        >
            {
                theme.theme === "light" ? 
                <Moon size={16} /> :
                <SunDim size={16} fill={"#fff"} />
            }
        </Button>
    
    )
}